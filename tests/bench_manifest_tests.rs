#[silo::test]
fn scan_tombstone_profile_bench_is_harness_free() {
    let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let manifest = std::fs::read_to_string(&manifest_path).expect("read Cargo.toml");
    let cargo_toml: toml::Value = toml::from_str(&manifest).expect("parse Cargo.toml");

    let benches = cargo_toml
        .get("bench")
        .and_then(toml::Value::as_array)
        .expect("Cargo.toml should contain [[bench]] entries");

    let scan_bench = benches
        .iter()
        .find(|bench| {
            bench.get("name").and_then(toml::Value::as_str) == Some("scan_tombstone_profile")
        })
        .expect("scan_tombstone_profile bench should be registered");

    assert_eq!(
        scan_bench.get("harness").and_then(toml::Value::as_bool),
        Some(false),
        "scan_tombstone_profile bench should run with harness = false"
    );
}
