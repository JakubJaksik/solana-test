fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "src/senders/jito/proto";
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                format!("{}/searcher.proto", proto_dir),
                format!("{}/bundle.proto", proto_dir),
                format!("{}/packet.proto", proto_dir),
                format!("{}/shared.proto", proto_dir),
            ],
            &[proto_dir],
        )?;
    println!("cargo:rerun-if-changed={}", proto_dir);
    Ok(())
}
