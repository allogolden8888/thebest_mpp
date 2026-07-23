// Компилирует те же platform-contracts/*.proto, что уже провалидированы
// `protoc --descriptor_set_out` (platform_contracts.md) — здесь тот же
// источник компилируется в реальный Rust-код через prost-build, доказывая,
// что контракты не только синтаксически валидны, но и реально
// потребляемы сервисом на заявленном для него языке.
fn main() {
    // Override для Docker-сборки (build context — корень репозитория, не
    // директория сервиса, монорепо-layout ломает относительный путь
    // "../../platform-contracts" внутри builder-стадии) — см. Dockerfile.
    let proto_root = std::env::var("PLATFORM_CONTRACTS_DIR")
        .unwrap_or_else(|_| "../../platform-contracts".to_string());
    prost_build::compile_protos(
        &[
            format!("{proto_root}/common/enums.proto"),
            format!("{proto_root}/common/types.proto"),
            format!("{proto_root}/common/stage_contract.proto"),
        ],
        &[&proto_root],
    )
    .expect("не удалось скомпилировать platform-contracts/common/*.proto");

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/stage_contract.proto");
}
