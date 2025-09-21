fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
    std::env::set_var("PROTOC", protoc);
    let proto_files = &["proto/silo.proto"];
    let includes = &["proto"];

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Let prost/tonic write into OUT_DIR per best practice
        .type_attribute(
            ".silo.v1.RetryPolicy",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(proto_files, includes)
        .expect("failed to compile protos");
}
