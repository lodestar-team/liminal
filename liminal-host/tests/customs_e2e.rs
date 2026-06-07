//! End-to-end acceptance test for the Customs compliance pipeline.
//!
//! Runs the real `liminal` binary over the offline fixtures and asserts the
//! showcase property: flagged transfers reach quarantine and NEVER reach the
//! system-of-record or Kafka. Requires the customs components to be built
//! (`just build` / cargo build --target wasm32-wasip2); skips cleanly if not.

use std::path::Path;
use std::process::Command;

const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

#[test]
fn flagged_transfers_never_reach_the_writer() {
    let manifest = format!("{REPO}/examples/customs/customs.pipeline.toml");
    let sor_wasm = format!("{REPO}/examples/customs/sink-sor.wasm");
    if !Path::new(&manifest).exists() || !Path::new(&sor_wasm).exists() {
        eprintln!("skipping: build the customs components first (`just build`)");
        return;
    }

    let output = Command::new(env!("CARGO_BIN_EXE_liminal"))
        .arg("examples/customs/customs.pipeline.toml")
        .current_dir(REPO)
        .output()
        .expect("run liminal");
    assert!(
        output.status.success(),
        "pipeline exited with error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let sor_and_kafka: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("SOR ") || l.starts_with("KAFKA "))
        .collect();
    let quarantine: Vec<&str> = stdout.lines().filter(|l| l.starts_with("QUARANTINE ")).collect();
    let hold: Vec<&str> = stdout.lines().filter(|l| l.starts_with("HOLD ")).collect();

    // The two flagged transfers (counterparty = sanctioned address).
    for tx in ["0xaa02", "0xbb02"] {
        assert!(
            quarantine.iter().any(|l| l.contains(tx)),
            "flagged {tx} must be quarantined"
        );
        assert!(
            !sor_and_kafka.iter().any(|l| l.contains(tx)),
            "compliance regression: flagged {tx} reached the SoR/Kafka writer"
        );
    }

    // The indeterminate transfer is held, not written.
    assert!(hold.iter().any(|l| l.contains("0xcc01")), "indeterminate 0xcc01 must be held");
    assert!(
        !sor_and_kafka.iter().any(|l| l.contains("0xcc01")),
        "compliance regression: indeterminate 0xcc01 reached the writer"
    );

    // Cleared transfers DO reach the system of record (sanity: the gate isn't
    // just dropping everything).
    assert!(sor_and_kafka.iter().any(|l| l.contains("0xaa01")), "cleared 0xaa01 must reach SoR");
}
