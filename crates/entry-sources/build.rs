fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/shredstream.proto", "proto/shared.proto"], &["proto"])
        .expect("compile shredstream protos");
    println!("cargo:rerun-if-changed=proto/shredstream.proto");
    println!("cargo:rerun-if-changed=proto/shared.proto");
}
