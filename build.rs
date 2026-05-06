use protoc_bin_vendored::protoc_bin_path;

fn main()->Result<(), Box<dyn std::error::Error>>{
    println!("cargo:rerun-if-changed=proto/broker.proto");
    println!("cargo:rerun-if-changed=build.rs");

    let protoc=protoc_bin_path().map_err(|e| format!("protoc not found: {}", e))?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure().build_server(true).build_client(true).compile_protos(&["proto/broker.proto"],&["proto"],).map_err(|e| format!("tonic_build failed: {}", e))?;

    println!("cargo:warning=✅ Cortex-MQ broker.proto compiled successfully!");
    Ok(())
}