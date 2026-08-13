// Тот же паттерн, что у остальных Rust-сервисов этого среза — см.
// services/destination-resolution-service/build.rs для полного обоснования.
fn main() {
    let proto_root = std::env::var("PLATFORM_CONTRACTS_DIR")
        .unwrap_or_else(|_| "../../platform-contracts".to_string());

    // Реальная находка (Docker build, protoc из apt protobuf-compiler):
    // system protoc не ищет google/protobuf/*.proto (well-known types)
    // автоматически, если ему явно переданы свои -I ("protoc failed:
    // google/protobuf/timestamp.proto: File not found"). На хосте это не
    // всплывало, т.к. Homebrew-проток кладёт WKT в bin/../include —
    // protoc проверяет этот путь неявно только когда явных -I вообще
    // нет. Добавляем известные расположения WKT явным include путём,
    // только если файл там реально есть — no-op там, где не нужно.
    let mut includes = vec![proto_root.clone()];
    for candidate in ["/usr/include", "/opt/homebrew/include", "/usr/local/include"] {
        if std::path::Path::new(candidate).join("google/protobuf/timestamp.proto").exists() {
            includes.push(candidate.to_string());
        }
    }
    prost_build::compile_protos(
        &[
            format!("{proto_root}/common/enums.proto"),
            format!("{proto_root}/common/types.proto"),
            format!("{proto_root}/common/stage_contract.proto"),
        ],
        &includes,
    )
    .expect("не удалось скомпилировать platform-contracts/common/*.proto");

    // config.changes (ConfigChangeEvent) — см. destination-resolution-service/build.rs
    // для полного обоснования отдельного вызова + extern_path.
    let mut events_config = prost_build::Config::new();
    events_config.extern_path(".mpp.common.v1", "crate::proto");
    events_config
        .compile_protos(&[format!("{proto_root}/events/config_and_control.proto")], &includes)
        .expect("не удалось скомпилировать platform-contracts/events/config_and_control.proto");

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/stage_contract.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/config_and_control.proto");
}
