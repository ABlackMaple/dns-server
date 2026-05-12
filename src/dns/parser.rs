use std::hash::{DefaultHasher, Hasher};

use smallvec::SmallVec;

use crate::dns::{HEADER_SIZE, OFF_ANCOUNT, OFF_ARCOUNT, OFF_NSCOUNT, OFF_QDCOUNT, ParsedQuery};

const POINTER_SIGN: u8 = 0xc0;

#[inline]
pub fn skip_name(buf: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        if pos > buf.len() {
            return None
        }

        let len = buf[pos] as usize;

        if len == 0 {
            return Some(pos + 1);
        }

        if len as u8 & POINTER_SIGN == POINTER_SIGN {
            if pos + 2 > buf.len() {
                return None;
            }
            return Some(pos + 2);
        }

        pos += len + 1;

    }

}

pub fn parse_query<'a>(buf: &'a [u8]) -> Option<ParsedQuery<'a>> {
    if buf.len() < HEADER_SIZE {
        return None;
    }

    let qdcount = u16::from_be_bytes([buf[OFF_QDCOUNT], buf[OFF_QDCOUNT + 1]]);
    if qdcount == 0 {
        return None
    }

    let mut pos = HEADER_SIZE;
    let mut hasher = DefaultHasher::new();
    let mut labels = SmallVec::new();

    loop {
        if pos >= buf.len() {
            return None
        }

        let len = buf[pos] as usize;

        if len as u8 & POINTER_SIGN == POINTER_SIGN {
            return None
        }

        hasher.write_u8(len as u8);

        pos += 1;
        if len == 0 {
            break;
        }

        if pos + len > buf.len() {
            return None
        }

        labels.push(&buf[pos..pos + len]);

        for b in &buf[pos..(pos + len)] {
            hasher.write_u8(b.to_ascii_lowercase());
        }

        pos += len;

    }

    if pos + 4 > buf.len() {
        return None
    }

    let qtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
    hasher.write_u16(qtype);

    let q_len = (pos + 4 - HEADER_SIZE) as u16;
    Some(
        ParsedQuery { cache_key: hasher.finish(), q_len, labels }
    )

}


pub fn find_ttl_offsets(buf: &[u8], tail_start: usize) -> Option<Vec<u16>> {
    if buf.len() < HEADER_SIZE {
        return None
    }

    let ancount = u16::from_be_bytes([buf[OFF_ANCOUNT], buf[OFF_ANCOUNT + 1]]);
    let nscount = u16::from_be_bytes([buf[OFF_NSCOUNT], buf[OFF_NSCOUNT + 1]]);
    let arcount = u16::from_be_bytes([buf[OFF_ARCOUNT], buf[OFF_ARCOUNT + 1]]);

    let total_count = (ancount + nscount + arcount) as usize;

    let mut offsets = Vec::with_capacity(total_count);

    let mut pos = tail_start;

    for _ in 0..total_count {
        pos = skip_name(buf, pos)?;

        if pos + 10 > buf.len() {
            return None
        }

        let ttl_pos = pos + 4;
        offsets.push((ttl_pos - tail_start) as u16);
        let rdlength = u16::from_be_bytes([buf[pos + 8], buf[pos + 9]]) as usize;
        pos += 10 + rdlength;
    }

    Some(offsets)

    
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dns::HEADER_SIZE;

    #[test]
    fn test_skip_name() {
        // 正常的域名：3 w w w 7 e x a m p l e 3 c o m 0
        let buf = b"\x03www\x07example\x03com\x00";
        assert_eq!(skip_name(buf, 0), Some(17)); // 1+3 + 1+7 + 1+3 + 1 = 17

        // 带有指针的域名: 3 w w w C0 0C (偏移量指针)
        let buf_ptr = b"\x03www\xC0\x0C";
        assert_eq!(skip_name(buf_ptr, 0), Some(6)); // 1+3 + 2 = 6

        // 越界的无效域名报文
        let buf_invalid = b"\x03www\x07exam";
        assert_eq!(skip_name(buf_invalid, 0), None);
    }

    #[test]
    fn test_parse_query() {
        let mut buf = vec![0u8; HEADER_SIZE];
        // 设置 QDCOUNT = 1
        buf[OFF_QDCOUNT] = 0;
        buf[OFF_QDCOUNT + 1] = 1;

        // 添加 Question 部分: \x03www\x01a\x03com\x00 (11字节)
        buf.extend_from_slice(b"\x03www\x01a\x03com\x00");
        // 添加 QTYPE (A: 1)
        buf.extend_from_slice(&[0, 1]);
        // 添加 QCLASS (IN: 1)
        buf.extend_from_slice(&[0, 1]);

        let res = parse_query(&buf);
        assert!(res.is_some());
        let ParsedQuery{cache_key, q_len, labels} = res.unwrap();
        assert_eq!(q_len, 11 + 4); // 域名长度11 + 4字节的Type和Class = 15
        assert_eq!(labels.as_slice(), &[b"www" as &[u8], b"a", b"com"]);
    }

    #[test]
    fn test_find_ttl_offsets() {
        let mut buf = vec![0u8; HEADER_SIZE];
        // 设置 ANCOUNT = 1
        buf[OFF_ANCOUNT] = 0;
        buf[OFF_ANCOUNT + 1] = 1;

        let tail_start = buf.len();
        // 伪造一个Answer记录: 域名指针(2字节) + TYPE(2) + CLASS(2) + TTL(4) + RDLEN(2) + RDATA(4)
        buf.extend_from_slice(b"\xC0\x0C"); // Name (Pointer)
        buf.extend_from_slice(&[0, 1, 0, 1]); // TYPE(A), CLASS(IN)
        let ttl_pos = buf.len(); 
        buf.extend_from_slice(&[0, 0, 0x02, 0x58]); // TTL = 600
        buf.extend_from_slice(&[0, 4, 1, 1, 1, 1]); // RDLEN=4, RDATA=1.1.1.1

        let offsets = find_ttl_offsets(&buf, tail_start).unwrap();
        assert_eq!(offsets, vec![( ttl_pos - tail_start ) as u16]);
    }
}