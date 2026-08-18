// Тот же паттерн, что services/policy-service/build.rs — компилирует
// настоящие platform-contracts/*.proto через prost-build. Нужны только
// common/enums.proto (ConfigEntityType) и events/config_and_control.proto
// (ConfigChangeEvent) — этот сервис не публикует stage-команды, только
// потребляет config.changes.
fn main() {
    let proto_root = std::env::var("PLATFORM_CONTRACTS_DIR")
        .unwrap_or_else(|_| "../../platform-contracts".to_string());

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
            format!("{proto_root}/events/config_and_control.proto"),
        ],
        &includes,
    )
    .expect("не удалось скомпилировать platform-contracts/{common,events}/*.proto");

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/events/config_and_control.proto");
}
