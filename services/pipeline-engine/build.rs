// Тот же паттерн, что у остальных Rust-сервисов этого среза — см.
// services/destination-resolution-service/build.rs.
fn main() {
    let proto_root = std::env::var("PLATFORM_CONTRACTS_DIR")
        .unwrap_or_else(|_| "../../platform-contracts".to_string());
    prost_build::compile_protos(
        &[
            format!("{proto_root}/common/enums.proto"),
            format!("{proto_root}/common/types.proto"),
            format!("{proto_root}/common/stage_contract.proto"),
            format!("{proto_root}/events/message_events.proto"),
        ],
        &[&proto_root],
    )
    .expect("не удалось скомпилировать platform-contracts/*.proto");

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/stage_contract.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/message_events.proto");
}
