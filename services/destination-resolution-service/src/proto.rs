//! prost генерирует один модуль на protobuf package (`mpp.common.v1`),
//! компилируя все три platform-contracts/common/*.proto из build.rs.
include!(concat!(env!("OUT_DIR"), "/mpp.common.v1.rs"));

/// `mpp.events.v1` (`platform-contracts/events/config_and_control.proto`) —
/// отдельный package, отдельный prost_build::compile_protos-вызов в
/// build.rs (extern_path заворачивает ссылки на mpp.common.v1 обратно на
/// crate::proto, а не на несуществующий nested-модуль).
pub mod events_v1 {
    include!(concat!(env!("OUT_DIR"), "/mpp.events.v1.rs"));
}
