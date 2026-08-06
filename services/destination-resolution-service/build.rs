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

    println!("cargo:rerun-if-changed={proto_root}/common/enums.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/types.proto");
    println!("cargo:rerun-if-changed={proto_root}/common/stage_contract.proto");
}
