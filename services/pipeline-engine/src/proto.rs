//! Два protobuf package в этом сервисе — mpp.common.v1 (stage-контракты) и
//! mpp.events.v1 (IncomingMessage) — mpp.events.v1 ссылается на типы
//! mpp.common.v1 (`SmsPayload`, `Channel`) относительными путями вида
//! `super::super::common::v1::X`, сгенерированными prost-build исходя из
//! ТОЧНОГО совпадения структуры модулей с точками в имени package.
//! Найдено компилятором, не предположено: первая версия (плоские модули
//! `common`/`events` без `mpp`-обёртки) не собиралась — `cannot find
//! common in super`, потому что относительные пути в сгенерированном коде
//! ожидают `crate::proto::mpp::common::v1`, не `crate::proto::common`.
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

// Короткие алиасы — весь остальной код сервиса писался под crate::proto::common::X.
pub use mpp::common::v1 as common;
pub use mpp::events::v1 as events;
