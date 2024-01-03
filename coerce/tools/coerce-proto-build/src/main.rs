use protobuf_codegen::Customize;
use std::path::Path;

struct ProtobufFile {
    pub proto_file: &'static str,
    pub output_dir: &'static str,
}

fn main() -> std::io::Result<()> {
    if !Path::new("coerce").exists() {
        panic!("could not find coerce root directory, please run from the coerce repository root");
    }

    compile_proto(
        "coerce/src/protocol/",
        vec![
            (
                "coerce/src/protocol/network.proto",
                "coerce/src/remote/net/proto",
            ),
            (
                "coerce/src/protocol/sharding.proto",
                "coerce/src/sharding/proto",
            ),
            (
                "coerce/src/protocol/singleton.proto",
                "coerce/src/remote/cluster/singleton/proto",
            ),
            (
                "coerce/src/protocol/persistent/journal.proto",
                "coerce/src/persistent/journal/proto",
            ),
        ]
        .into_iter(),
    );

    compile_proto(
        "examples/coerce-sharded-chat-example/src/protocol/",
        vec![(
            "examples/coerce-sharded-chat-example/src/protocol/chat.proto",
            "examples/coerce-sharded-chat-example/src/protocol",
        )]
        .into_iter(),
    );

    Ok(())
}

fn compile_proto<I: Iterator<Item = (&'static str, &'static str)>>(
    include_dir: &'static str,
    protobuf_files: I,
) {
    for file in protobuf_files {
        protobuf_codegen::Codegen::new()
            .customize(Customize::default().gen_mod_rs(true))
            .out_dir(file.1)
            .input(file.0)
            .include(include_dir)
            .run()
            .unwrap_or_else(|e| panic!("protoc {}, error={}", file.0, e));
    }
}
