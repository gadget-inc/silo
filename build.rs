fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
    std::env::set_var("PROTOC", protoc);
    let proto_files = &["proto/silo.proto", "proto/membership.proto"];
    let includes = &["proto"];

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Let prost/tonic write into OUT_DIR per best practice
        .type_attribute(
            ".silo.v1.RetryPolicy",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.v1.ShardMap",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.v1.ShardAssignment",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        // Membership protobuf types with serde support
        .type_attribute(
            ".silo.membership.v1.SetRequest",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.Response",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.Node",
            "#[derive(serde::Serialize, serde::Deserialize, Eq)]",
        )
        .type_attribute(
            ".silo.membership.v1.Entry",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.LeaderId",
            "#[derive(serde::Serialize, serde::Deserialize, Eq)]",
        )
        .type_attribute(
            ".silo.membership.v1.Vote",
            "#[derive(serde::Serialize, serde::Deserialize, Eq)]",
        )
        .type_attribute(
            ".silo.membership.v1.LogId",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.Membership",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.NodeIdSet",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".silo.membership.v1.StateMachineData",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(proto_files, includes)
        .expect("failed to compile protos");
}
