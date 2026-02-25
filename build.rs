fn main() {
    // Tell cargo to only rerun this build script if the proto files change
    println!("cargo:rerun-if-changed=proto/silo.proto");
    println!("cargo:rerun-if-changed=proto/gubernator.proto");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
    // SAFETY: This is safe because the build script runs single-threaded before any
    // parallel compilation, so there's no concurrent access to environment variables.
    unsafe { std::env::set_var("PROTOC", protoc) };
    let includes = &["proto"];

    // Generate file descriptor set for gRPC reflection
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("silo_descriptor.bin");

    // Compile silo.proto with file descriptor set for reflection
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(&descriptor_path)
        .type_attribute(
            ".silo.v1.RetryPolicy",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.v1.RateLimitRetryPolicy",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&["proto/silo.proto"], includes)
        .expect("failed to compile silo.proto");

    // Compile gubernator.proto for the rate limit client
    tonic_build::configure()
        .build_client(true)
        .build_server(false) // We only need the client for gubernator
        .compile_protos(&["proto/gubernator.proto"], includes)
        .expect("failed to compile gubernator.proto");

    // Compile FlatBuffers schema for internal storage codec
    println!("cargo:rerun-if-changed=schema/internal_storage.fbs");
    let flatc_status = std::process::Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg(out_dir.as_os_str())
        .arg("schema/internal_storage.fbs")
        .status()
        .expect(
            "failed to run flatc - ensure flatbuffers is installed (available via nix devshell)",
        );
    assert!(flatc_status.success(), "flatc failed to compile schema");
}
