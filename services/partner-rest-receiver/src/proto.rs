//! Два protobuf package — mpp.common.v1 (`SmsPayload`, `Channel`, `PartnerContext`)
//! и mpp.events.v1 (`IncomingMessage`, `ExecutionControlRecord`) — та же находка компилятора, что уже
//! задокументирована в `services/pipeline-engine/src/proto.rs`: prost генерирует
//! cross-package ссылки как `super::super::common::v1::X`, что требует точного
//! совпадения структуры модулей с точками в имени package.
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
