use std::env;
use std::fs;

use protobuf_codegen_pure::Customize;
use std::path::Path;

fn main() -> std::io::Result<()> {
    // let path = env::current_dir()?.display();
    // protobuf_codegen_pure::Codegen::new()
    //     .customize(Cusetomize {
    //         gen_mod_rs: Some(true),
    //         ..Default::default()
    //     })
    //     .out_dir("src/remote/net/proto")
    //     .input("src/remote/net/proto/protocol.proto")
    //     .include("src/remote/net/proto")
    //     .run()
    //     .expect("protoc");

    Ok(())
}