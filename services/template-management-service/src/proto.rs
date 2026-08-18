//! Два protobuf package — mpp.common.v1 (ConfigEntityType) и mpp.events.v1
//! (ConfigChangeEvent, ссылается на ConfigEntityType из common.v1) — тот же
//! паттерн вложенных модулей `mpp::{common,events}::v1`, что в
//! services/pipeline-engine/src/proto.rs (см. этот файл за развёрнутым
//! обоснованием: плоские модули без `mpp`-обёртки не компилируются, prost
//! генерирует относительные пути вида `super::super::common::v1::X`,
//! которые требуют точного совпадения структуры модулей).
pub mod mpp {
    pub mod common {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/mpp.common.v1.rs"));
        }
    }
    pub mod events {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/mpp.events.v1.rs"));
        }
    }
}

pub use mpp::common::v1 as common;
pub use mpp::events::v1 as events;
