
/// DNS 头部长度固定为 12 字节
pub const HEADER_SIZE: usize = 12;

/// DNS 报文头部偏移量常量
pub const OFF_ID: usize = 0;
pub const OFF_FLAGS: usize = 2;
pub const OFF_QDCOUNT: usize = 4;
pub const OFF_ANCOUNT: usize = 6;
pub const OFF_NSCOUNT: usize = 8;
pub const OFF_ARCOUNT: usize = 10;

/// 标志位掩码
pub const FLAG_QR: u16 = 0x8000; // 1 为响应，0 为查询
pub const FLAG_TC: u16 = 0x0200; // 截断标志
pub const FLAG_RD: u16 = 0x0100; // 期望递归

/// 常见的 DNS 记录类型 (精简版)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u16)]
pub enum RecordType {
    A = 1,
    CNAME = 5,
    SOA = 6,
    PTR = 12,
    MX = 15,
    TXT = 16,
    AAAA = 28,
    SRV = 33,
    ANY = 255,
    Unknown(u16),
}

impl From<u16> for RecordType {
    fn from(val: u16) -> Self {
        match val {
            1 => RecordType::A,
            5 => RecordType::CNAME,
            6 => RecordType::SOA,
            12 => RecordType::PTR,
            15 => RecordType::MX,
            16 => RecordType::TXT,
            28 => RecordType::AAAA,
            33 => RecordType::SRV,
            255 => RecordType::ANY,
            _ => RecordType::Unknown(val),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_from_u16() {
        assert_eq!(RecordType::from(1), RecordType::A);
        assert_eq!(RecordType::from(5), RecordType::CNAME);
        assert_eq!(RecordType::from(28), RecordType::AAAA);
        assert_eq!(RecordType::from(255), RecordType::ANY);
        assert_eq!(RecordType::from(999), RecordType::Unknown(999));
    }
}