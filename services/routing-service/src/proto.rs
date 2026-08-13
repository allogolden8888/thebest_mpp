//! См. services/destination-resolution-service/src/proto.rs — тот же паттерн.
include!(concat!(env!("OUT_DIR"), "/mpp.common.v1.rs"));

pub mod events_v1 {
    include!(concat!(env!("OUT_DIR"), "/mpp.events.v1.rs"));
}
