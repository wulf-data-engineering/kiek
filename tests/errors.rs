use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
fn invalid_argument() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg("--foo");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));

    Ok(())
}