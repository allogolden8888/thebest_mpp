//! `check_ip_and_application` (service_internal_methods.md §1.1) — IPv4 CIDR
//! allowlist-проверка + проверка, что `application_id` разрешает нужный канал
//! (`allowed_channels`). Ручной CIDR-парсер на `std::net::Ipv4Addr`, без
//! внешнего крейта (`ipnetwork` и т.п.) — `ip_allowlist` в `partner.schema.json`
//! это только IPv4 CIDR (`"185.65.212.0/24"`), задача маленькая и полностью
//! покрывается стандартной библиотекой.

use std::net::Ipv4Addr;

#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    network: u32,
    prefix_len: u32,
}

impl Cidr {
    pub fn parse(s: &str) -> Option<Self> {
        let (addr_part, prefix_part) = s.split_once('/')?;
        let addr: Ipv4Addr = addr_part.parse().ok()?;
        let prefix_len: u32 = prefix_part.parse().ok()?;
        if prefix_len > 32 {
            return None;
        }
        Some(Self { network: u32::from(addr), prefix_len })
    }

    pub fn contains(&self, addr: Ipv4Addr) -> bool {
        if self.prefix_len == 0 {
            return true;
        }
        let mask = u32::MAX << (32 - self.prefix_len);
        (u32::from(addr) & mask) == (self.network & mask)
    }
}

pub fn ip_allowed(allowlist: &[String], remote_ip: Ipv4Addr) -> bool {
    allowlist.iter().filter_map(|s| Cidr::parse(s)).any(|cidr| cidr.contains(remote_ip))
}

pub fn channel_allowed(allowed_channels: &[String], channel: &str) -> bool {
    allowed_channels.iter().any(|c| c == channel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_inside_cidr_matches() {
        let cidr = Cidr::parse("185.65.212.0/24").unwrap();
        assert!(cidr.contains("185.65.212.55".parse().unwrap()));
    }

    #[test]
    fn address_outside_cidr_does_not_match() {
        let cidr = Cidr::parse("185.65.212.0/24").unwrap();
        assert!(!cidr.contains("185.65.213.1".parse().unwrap()));
    }

    #[test]
    fn exact_host_cidr_slash_32() {
        let cidr = Cidr::parse("10.0.0.5/32").unwrap();
        assert!(cidr.contains("10.0.0.5".parse().unwrap()));
        assert!(!cidr.contains("10.0.0.6".parse().unwrap()));
    }

    #[test]
    fn invalid_cidr_string_returns_none() {
        assert!(Cidr::parse("not-an-ip/24").is_none());
        assert!(Cidr::parse("10.0.0.0/33").is_none());
        assert!(Cidr::parse("10.0.0.0").is_none(), "без /prefix — не CIDR");
    }

    #[test]
    fn ip_allowed_checks_against_whole_allowlist() {
        let allowlist = vec!["185.65.212.0/24".to_string(), "10.0.0.0/8".to_string()];
        assert!(ip_allowed(&allowlist, "10.1.2.3".parse().unwrap()));
        assert!(ip_allowed(&allowlist, "185.65.212.1".parse().unwrap()));
        assert!(!ip_allowed(&allowlist, "8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn ip_allowed_ignores_malformed_entries_instead_of_panicking() {
        let allowlist = vec!["garbage".to_string(), "10.0.0.0/8".to_string()];
        assert!(ip_allowed(&allowlist, "10.1.2.3".parse().unwrap()));
    }

    #[test]
    fn channel_allowed_checks_membership() {
        let allowed = vec!["SMS".to_string()];
        assert!(channel_allowed(&allowed, "SMS"));
        assert!(!channel_allowed(&allowed, "EMAIL"));
    }
}
