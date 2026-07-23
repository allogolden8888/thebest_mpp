//! prost генерирует один модуль на protobuf package (`mpp.common.v1`),
//! компилируя все три platform-contracts/common/*.proto из build.rs.
include!(concat!(env!("OUT_DIR"), "/mpp.common.v1.rs"));
