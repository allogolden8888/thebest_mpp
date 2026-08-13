// Тот же паттерн, что у остальных Rust-сервисов этого среза — см.
// services/destination-resolution-service/build.rs.
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
    // config_and_control.proto (ConfigChangeEvent) — тот же package mpp.events.v1,
    // что message_events.proto, поэтому просто добавлен в тот же вызов (мержится
    // в тот же mpp.events.v1.rs) — не нужен отдельный compile_protos/extern_path,
    // как в сервисах с плоской (не nested) раскладкой модулей (см.
    // destination-resolution-service/build.rs).
    prost_build::compile_protos(
        &[
            format!("{proto_root}/common/enums.proto"),
            format!("{proto_root}/common/types.proto"),
            format!("{proto_root}/common/stage_contract.proto"),
            format!("{proto_root}/events/message_events.proto"),
            format!("{proto_root}/events/config_and_control.proto"),
            // Retry-until-expiry (dynamic-seeking-russell.md): STAGE_RETRY
            // SchedulerBackgroundTask — та же mpp.events.v1, тот же вызов.
            format!("{proto_root}/events/scheduler_events.proto"),
        ],
        &includes,
    )
    .expect("не удалось скомпилировать platform-contracts/*.proto");

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/stage_contract.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/message_events.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/config_and_control.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/scheduler_events.proto");
}
