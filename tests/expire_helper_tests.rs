//! Tests for the terminal-job expiration settings plumb.
//!
//! The `expire_terminal_job_records` helper itself and the
//! `set_job_status_with_index_opts` writer are `pub(crate)` and exercised
//! end-to-end by the cancel / import / completion integration tests added
//! in the follow-up retention PRs. This file pins the wiring that those
//! PRs depend on: that the two new `DatabaseConfig` / `DatabaseTemplate`
//! settings deserialize correctly, default to disabled, and flow into a
//! freshly opened shard without panicking.

mod test_helpers;

use silo::settings::{AppConfig, DatabaseConfig, DatabaseTemplate};
use test_helpers::open_temp_shard;

#[silo::test]
fn database_template_expire_settings_default_to_none() {
    let t = DatabaseTemplate::default();
    assert_eq!(t.completed_job_expire_s, None);
    assert_eq!(t.terminal_job_expire_s, None);
}

#[silo::test]
fn database_config_expire_settings_default_to_none() {
    let c = DatabaseConfig::default();
    assert_eq!(c.completed_job_expire_s, None);
    assert_eq!(c.terminal_job_expire_s, None);
}

#[silo::test]
fn parse_toml_expire_settings_omitted_yields_none() {
    let toml_str = r#"
[database]
backend = "fs"
path = "/tmp/silo-%shard%"
"#;
    let cfg: AppConfig = toml::from_str(toml_str).expect("parse TOML");
    assert_eq!(cfg.database.completed_job_expire_s, None);
    assert_eq!(cfg.database.terminal_job_expire_s, None);
}

#[silo::test]
fn parse_toml_expire_settings_round_trip() {
    let toml_str = r#"
[database]
backend = "fs"
path = "/tmp/silo-%shard%"
completed_job_expire_s = 86400
terminal_job_expire_s = 604800
"#;
    let cfg: AppConfig = toml::from_str(toml_str).expect("parse TOML");
    assert_eq!(cfg.database.completed_job_expire_s, Some(86400));
    assert_eq!(cfg.database.terminal_job_expire_s, Some(604800));
}

#[silo::test]
async fn shard_opens_with_expire_settings_unset() {
    // Smoke test: with no TTL configured (matches main today), opening a
    // shard must continue to succeed.
    let (_tmp, _shard) = open_temp_shard().await;
}
