use std::{io::IoSlice, time::Instant};

use crate::{Result, cache::CachedTail, dns::{FLAG_QR, FLAG_RD, HEADER_SIZE, OFF_ANCOUNT, OFF_ARCOUNT, OFF_FLAGS, OFF_NSCOUNT}};

pub fn build_header_slice<'a>(req_buf: &mut [u8], req_len: usize, question_len: u16, cached: &'a CachedTail) -> Result<()> {
    let req_header_size = HEADER_SIZE + question_len as usize;
    if req_header_size > req_len {
        return Err("Request too short".into());
    }

    let mut flags = u16::from_be_bytes([req_buf[OFF_FLAGS], req_buf[OFF_FLAGS + 1]]);
    flags |= FLAG_QR;
    flags |= FLAG_RD;

    let flags_bytes = flags.to_be_bytes();
    req_buf[OFF_FLAGS] = flags_bytes[0];
    req_buf[OFF_FLAGS + 1] = flags_bytes[1];

    req_buf[OFF_ANCOUNT..OFF_ANCOUNT + 2].copy_from_slice(&cached.answer_count.to_be_bytes());
    req_buf[OFF_NSCOUNT..OFF_NSCOUNT + 2].copy_from_slice(&cached.authority_count.to_be_bytes());
    req_buf[OFF_ARCOUNT..OFF_ARCOUNT + 2].copy_from_slice(&cached.additional_count.to_be_bytes());

    return Ok(())
}


// pub fn build_cached_response<'a>(req_buf: &'a mut [u8], req_len: usize, question_len: u16, cached: &CachedTail) -> Option<&'a [u8]> {
//     let req_header_size = HEADER_SIZE + question_len as usize;
//     if req_header_size > req_len {
//         return None;
//     }

//     let total_len = req_header_size + cached.tail_bytes.len();

//     if req_buf.len() < total_len {
//         return None;
//     }

//     let mut flags = u16::from_be_bytes([req_buf[OFF_FLAGS], req_buf[OFF_FLAGS + 1]]);
//     flags |= FLAG_QR;
//     flags |= FLAG_RD;

//     let flags_bytes = flags.to_be_bytes();
//     req_buf[OFF_FLAGS] = flags_bytes[0];
//     req_buf[OFF_FLAGS + 1] = flags_bytes[1];

//     req_buf[OFF_ANCOUNT..OFF_ANCOUNT + 2].copy_from_slice(&cached.answer_count.to_be_bytes());
//     req_buf[OFF_NSCOUNT..OFF_NSCOUNT + 2].copy_from_slice(&cached.authority_count.to_be_bytes());
//     req_buf[OFF_ARCOUNT..OFF_ARCOUNT + 2].copy_from_slice(&cached.additional_count.to_be_bytes());

//     req_buf[req_header_size..total_len].copy_from_slice(&cached.tail_bytes);

//     let now = Instant::now();

//     let remaining_ttl = if cached.expires_at > now {
//         (cached.expires_at - now).as_secs() as u32
//     } else {
//         0
//     };

//     let ttl_bytes = remaining_ttl.to_be_bytes();

//     for &offset in &cached.ttl_offsets {
//         let absolute_pos = req_header_size + offset as usize;
//         if absolute_pos + 4 <= total_len {
//             req_buf[absolute_pos..absolute_pos + 4].copy_from_slice(&ttl_bytes);
//         }
//     }
    
//     Some(&req_buf[..total_len])
    
// }
