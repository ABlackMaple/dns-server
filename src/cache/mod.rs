use std::{sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::{Duration, SystemTime, UNIX_EPOCH}};

use bytes::Bytes;
use std::time::Instant;

use crate::dns::{OFF_ANCOUNT, OFF_ARCOUNT, OFF_NSCOUNT, parser::find_ttl_offsets};

pub mod fast_db;
pub mod purge;
mod hosts;

#[inline]
pub(crate) fn now_sec() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

pub struct CachedTail {
    pub tail_bytes: Bytes,

//    pub ttl_offsets: Vec<u16>,

    pub expires_at: Instant,

    pub q_len: u16,

    pub answer_count: u16,
    pub authority_count: u16,
    pub additional_count: u16,

    pub hits: AtomicUsize,
    pub last_accessed_sec: AtomicU64,
}

impl CachedTail {
    pub fn new(buf: Bytes, tail_start: usize, q_len: u16) -> Self {
        let ttl_offsets = find_ttl_offsets(&buf, tail_start).unwrap();

        // let tail_bytes = buf.split_off(tail_start);
        let tail_bytes = Bytes::copy_from_slice(&buf[tail_start..]);
        let ancount = u16::from_be_bytes([buf[OFF_ANCOUNT], buf[OFF_ANCOUNT + 1]]);
        let nscount = u16::from_be_bytes([buf[OFF_NSCOUNT], buf[OFF_NSCOUNT + 1]]);
        let arcount = u16::from_be_bytes([buf[OFF_ARCOUNT], buf[OFF_ARCOUNT + 1]]);

        let hits = AtomicUsize::new(1);
        let last_accessed_sec = AtomicU64::new(now_sec());


        let ttl = ttl_offsets.iter().map(|ttl| if *ttl > 0 { *ttl } else { 60 }).min().unwrap_or(60).max(60) / 2;

        let expires_at = Instant::now() + Duration::from_secs(ttl as u64);

        Self {
            tail_bytes,
            expires_at,
            q_len: q_len as u16,
            answer_count: ancount,
            authority_count: nscount,
            additional_count: arcount,
            hits,
            last_accessed_sec,
        }
    }

    pub fn record_access(&self) {
        self.force_attenuation();
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn force_attenuation(&self) {
        let now = now_sec();
        let last = self.last_accessed_sec.load(Ordering::Relaxed);
        
        if last < now {
            let elapsed = (now - last) / 60;
            if elapsed > 0 {
                let cur_hits = self.hits.load(Ordering::Relaxed);
                let decayed_hist = cur_hits >> elapsed.min(31);
                self.hits.store(decayed_hist, Ordering::Relaxed);
                self.last_accessed_sec.store(now, Ordering::Relaxed);
            }
        }
    }
}

pub trait CacheStore: Send + Sync {
    
    async fn get(&self, key: u64, q_len: u16) -> Option<Arc<CachedTail>>;
    async fn insert(&self, key: u64, tail: Arc<CachedTail>);
    async fn purge_expired(&self) -> usize;
    async fn len(&self) -> usize;

}