//! Content addressing + composition signing (W1+ / W8).
//!
//! A Liminal manifest mixes two things: the *topology* (which components, with
//! which capabilities, wired how) and the *runtime config* (RPC URLs, DB creds —
//! the `${VAR}` values). The compliance attestation is about the former only.
//!
//! `compose` reduces a manifest to a **canonical composition**: component ids
//! paired with the `sha256` of their actual wasm bytes, their capability
//! declarations, the edge set, and the structural source filter — with every
//! secret excluded. That canonical form is hashed and signed. Verifying a
//! signature proves "this exact topology, with these capability boundaries and
//! these component binaries, was attested" — regardless of which endpoints it
//! later runs against.

use std::path::Path;

use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::manifest::Manifest;

// ---------------------------------------------------------------------------
// Canonical composition — the signed body. Field order here IS the canonical
// order; vectors are sorted before serialization so the bytes are deterministic.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct CanonicalSource {
    kind: String,
    topics: Vec<String>,
    addresses: Vec<String>,
}

#[derive(Serialize)]
struct CanonicalComponent {
    id: String,
    /// sha256 of the component's wasm bytes (hex) — the content address.
    sha256: String,
    capabilities: Vec<String>,
    /// HTTP origin allow-list — a capability boundary, so part of the attestation.
    allow_origins: Vec<String>,
}

#[derive(Serialize)]
struct CanonicalEdge {
    from: String,
    to: String,
    when: Option<String>,
}

#[derive(Serialize)]
struct Canonical {
    name: String,
    source: CanonicalSource,
    components: Vec<CanonicalComponent>,
    edges: Vec<CanonicalEdge>,
}

/// Build the canonical composition for a manifest, computing each component's
/// content address from its wasm file on disk.
fn canonicalize(manifest: &Manifest) -> Result<Canonical> {
    let mut topics = manifest.source.topics.clone();
    topics.sort();
    let mut addresses = manifest.source.addresses.clone();
    addresses.sort();

    let mut components = Vec::with_capacity(manifest.nodes.len());
    for node in &manifest.nodes {
        let mut capabilities = node.capabilities.clone();
        capabilities.sort();
        let mut allow_origins = node.allow_origins.clone();
        allow_origins.sort();
        components.push(CanonicalComponent {
            id: node.id.clone(),
            sha256: file_sha256(&node.wasm)
                .with_context(|| format!("hashing component {:?}", node.id))?,
            capabilities,
            allow_origins,
        });
    }
    components.sort_by(|a, b| a.id.cmp(&b.id));

    let mut edges: Vec<CanonicalEdge> = manifest
        .edges
        .iter()
        .map(|e| CanonicalEdge { from: e.from.clone(), to: e.to.clone(), when: e.when.clone() })
        .collect();
    edges.sort_by(|a, b| (&a.from, &a.to, &a.when).cmp(&(&b.from, &b.to, &b.when)));

    Ok(Canonical {
        name: manifest.name.clone(),
        source: CanonicalSource { kind: manifest.source.kind.clone(), topics, addresses },
        components,
        edges,
    })
}

/// The canonical bytes (what gets signed) and their sha256 digest (the
/// human-facing "composition hash").
fn canonical_bytes_and_hash(manifest: &Manifest) -> Result<(Vec<u8>, String)> {
    let canonical = canonicalize(manifest)?;
    let bytes = serde_json::to_vec(&canonical).context("serializing canonical composition")?;
    let hash = hex::encode(Sha256::digest(&bytes));
    Ok((bytes, hash))
}

fn file_sha256(path: &str) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {path}"))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

/// `compose hash` — print each component's content address and the composition
/// hash. Cross-checks any `sha256` already declared in the manifest.
pub fn hash(manifest_path: &str) -> Result<()> {
    let manifest = Manifest::load_lenient(manifest_path)?;
    let canonical = canonicalize(&manifest)?;

    println!("composition: {}", manifest.name);
    println!("components (content addresses):");
    for c in &canonical.components {
        println!("  {:<18} sha256:{}", c.id, c.sha256);
    }

    // Cross-check declared sha256 fields, if present.
    for node in &manifest.nodes {
        if let Some(declared) = &node.sha256 {
            let actual = file_sha256(&node.wasm)?;
            if declared != &actual {
                bail!(
                    "declared sha256 for {:?} does not match its wasm:\n  declared: {declared}\n  actual:   {actual}",
                    node.id
                );
            }
        }
    }

    let (_, composition_hash) = canonical_bytes_and_hash(&manifest)?;
    println!("\ncomposition hash: sha256:{composition_hash}");
    Ok(())
}

/// `compose keygen` — write `<out>.key` (secret seed) and `<out>.pub` (public
/// key), both hex. Minisign/cosign are the production path; this keeps the
/// example dependency-light.
pub fn keygen(out_prefix: &str) -> Result<()> {
    let mut seed = [0u8; 32];
    getrandom::getrandom(&mut seed).map_err(|e| anyhow::anyhow!("rng: {e}"))?;
    let signing = SigningKey::from_bytes(&seed);
    let verifying = signing.verifying_key();

    let key_path = format!("{out_prefix}.key");
    let pub_path = format!("{out_prefix}.pub");
    if Path::new(&key_path).exists() {
        bail!("{key_path} already exists; refusing to overwrite a signing key");
    }
    std::fs::write(&key_path, hex::encode(signing.to_bytes()))
        .with_context(|| format!("writing {key_path}"))?;
    std::fs::write(&pub_path, hex::encode(verifying.to_bytes()))
        .with_context(|| format!("writing {pub_path}"))?;

    println!("wrote {key_path} (keep secret) and {pub_path}");
    Ok(())
}

/// `compose sign` — sign the canonical composition; write `<manifest>.sig`.
pub fn sign(manifest_path: &str, key_path: &str) -> Result<()> {
    let manifest = Manifest::load_lenient(manifest_path)?;
    let (bytes, composition_hash) = canonical_bytes_and_hash(&manifest)?;

    let signing = load_signing_key(key_path)?;
    let sig = signing.sign(&bytes);

    let sig_path = format!("{manifest_path}.sig");
    std::fs::write(&sig_path, hex::encode(sig.to_bytes()))
        .with_context(|| format!("writing {sig_path}"))?;

    println!("signed composition sha256:{composition_hash}");
    println!("wrote {sig_path}");
    Ok(())
}

/// `compose verify` — recompute the canonical composition, check the signature,
/// and cross-check any declared component `sha256`. Errors (non-zero exit) on
/// any mismatch, so it slots into CI / a pre-run gate.
pub fn verify(manifest_path: &str, sig_path: &str, pub_path: &str) -> Result<()> {
    let manifest = Manifest::load_lenient(manifest_path)?;
    let (bytes, composition_hash) = canonical_bytes_and_hash(&manifest)?;

    let verifying = load_verifying_key(pub_path)?;
    let sig = load_signature(sig_path)?;

    verifying
        .verify(&bytes, &sig)
        .map_err(|e| anyhow::anyhow!("signature verification FAILED: {e}"))?;

    // Belt and braces: declared content addresses must match the wasm on disk.
    for node in &manifest.nodes {
        if let Some(declared) = &node.sha256 {
            let actual = file_sha256(&node.wasm)?;
            if declared != &actual {
                bail!("component {:?} wasm does not match its declared sha256", node.id);
            }
        }
    }

    println!("OK: signature valid for composition sha256:{composition_hash}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Key / signature IO
// ---------------------------------------------------------------------------

fn read_hex_array<const N: usize>(path: &str, what: &str) -> Result<[u8; N]> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {what} {path}"))?;
    let bytes = hex::decode(text.trim()).with_context(|| format!("decoding {what} hex"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{what} {path} must be exactly {N} bytes"))
}

fn load_signing_key(path: &str) -> Result<SigningKey> {
    Ok(SigningKey::from_bytes(&read_hex_array::<32>(path, "signing key")?))
}

fn load_verifying_key(path: &str) -> Result<VerifyingKey> {
    VerifyingKey::from_bytes(&read_hex_array::<32>(path, "public key")?)
        .context("invalid public key")
}

fn load_signature(path: &str) -> Result<Signature> {
    Ok(Signature::from_bytes(&read_hex_array::<64>(path, "signature")?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Edge, Manifest, NodeSpec, SourceSpec};
    use std::collections::BTreeMap;

    // A wasm that exists relative to the crate dir (where unit tests run), so
    // canonicalization can hash a real file without depending on the cwd.
    const A_WASM: &str = "../examples/customs/sink-sor.wasm";

    fn synthetic_manifest() -> Manifest {
        Manifest {
            name: "synthetic".into(),
            source: SourceSpec {
                kind: "fixture".into(),
                rpc: None,
                topics: vec![],
                addresses: vec![],
                path: Some("x.jsonl".into()),
            },
            nodes: vec![NodeSpec {
                id: "n".into(),
                wasm: A_WASM.into(),
                sha256: None,
                capabilities: vec!["stdout".into()],
                allow_origins: vec![],
                env: BTreeMap::new(),
            }],
            edges: vec![Edge { from: "source".into(), to: "n".into(), when: None }],
            dashboard: None,
        }
    }

    #[test]
    fn composition_hash_is_deterministic() {
        if !Path::new(A_WASM).exists() {
            eprintln!("skipping: build customs components first");
            return;
        }
        let m = synthetic_manifest();
        let (_, h1) = canonical_bytes_and_hash(&m).unwrap();
        let (_, h2) = canonical_bytes_and_hash(&m).unwrap();
        assert_eq!(h1, h2, "composition hash must be stable across runs");
        assert_eq!(h1.len(), 64, "sha256 hex is 64 chars");
    }

    #[test]
    fn sign_then_verify_roundtrips_and_detects_tampering() {
        // Pure crypto over arbitrary bytes — no files needed.
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).unwrap();
        let signing = SigningKey::from_bytes(&seed);
        let verifying = signing.verifying_key();

        let body = b"canonical composition bytes";
        let sig = signing.sign(body);
        assert!(verifying.verify(body, &sig).is_ok(), "honest verify must pass");

        let mut tampered = body.to_vec();
        tampered[0] ^= 0xff;
        assert!(verifying.verify(&tampered, &sig).is_err(), "tampered body must fail");
    }
}
