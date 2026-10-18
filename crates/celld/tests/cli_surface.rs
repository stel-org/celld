// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Public command-line compatibility boundaries.

use std::process::{Command, Output};

fn celld(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_celld"))
        .args(arguments)
        // Rejection must happen in argument parsing, before a caller's
        // environment can select a bucket or any other fleet configuration.
        .env_remove("CELLD_BUCKET")
        .env_remove("CELLD_TEST_BUCKET")
        .env_remove("CELLD_CLOUD")
        .env_remove("CELLD_ADDR")
        .env_remove("CELLD_INTERNAL_ADDR")
        .env_remove("CELLD_ADVERTISE")
        .env_remove("CELLD_INTERNAL_DEV_STORE")
        .output()
        .expect("run celld executable")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn retired_product_commands_are_rejected_before_runtime_setup() {
    for arguments in [
        ["d1"].as_slice(),
        ["d1", "execute"].as_slice(),
        ["kv"].as_slice(),
        ["kv", "get"].as_slice(),
        ["r2"].as_slice(),
        ["r2", "list"].as_slice(),
        ["queue"].as_slice(),
        ["queue", "info"].as_slice(),
    ] {
        let output = celld(arguments);
        assert!(
            !output.status.success(),
            "{arguments:?} unexpectedly succeeded"
        );
        assert!(
            stderr(&output).contains("unknown command or option"),
            "{arguments:?} did not return the parser's unsupported-command error: {}",
            stderr(&output)
        );
    }
}

#[test]
fn help_lists_supported_commands_but_not_retired_product_commands() {
    let output = celld(&["--help"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let help = String::from_utf8_lossy(&output.stdout);
    for command in ["celld deploy", "celld dev", "celld cell", "celld diagnose"] {
        assert!(help.contains(command), "missing {command} from help");
    }
    for command in ["celld d1", "celld kv", "celld queue", "celld r2"] {
        assert!(!help.contains(command), "retired {command} remains in help");
    }
}

#[test]
fn supported_help_and_version_commands_remain_available() {
    for arguments in [
        ["--version"].as_slice(),
        ["cell", "--help"].as_slice(),
        ["deploy", "--help"].as_slice(),
        ["dev", "--help"].as_slice(),
    ] {
        let output = celld(arguments);
        assert!(
            output.status.success(),
            "{arguments:?} failed: {}",
            stderr(&output)
        );
    }
}
