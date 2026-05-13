use std::io::IoSlice;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::watch;

use crate::cache::CachedTail;
use crate::cache::{CacheStore, fast_db::ShardedLfuDb};
use crate::dns::{HEADER_SIZE, ParsedQuery};
use crate::dns::parser::parse_query;
use crate::dns::wire::build_header_slice;
use crate::upstream::UpstreamManager;


#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FlightStatus {
    InFlight,
    Success,
    Failed,
}

pub struct FlightGroup {
    flights: Arc<DashMap<u64, watch::Receiver<FlightStatus>>>,
}

impl FlightGroup {
    pub fn new() -> Self {
        Self {
            flights: Arc::new(DashMap::new())
        }
    }

    pub async fn wait_or_execute<F, Fut>(&self, key: u64, fetch_func: F) -> bool 
        where 
            F: FnOnce() -> Fut,
            Fut: std::future::Future<Output = bool>,
    {
        // 快速进行锁操作并立即释放锁，绝对不在持有锁期间进行 .await
        let (mut rx, is_leader, tx) = {
            if let Some(sender_ref) = self.flights.get(&key) {
                (sender_ref.value().clone(), false, None)
            } else {
                let (tx, rx) = watch::channel(FlightStatus::InFlight);
                match self.flights.entry(key) {
                    dashmap::Entry::Occupied(entry) => {
                        (entry.get().clone(), false, None)
                    }
                    dashmap::Entry::Vacant(entry) => {
                        entry.insert(rx.clone());
                        (rx, true, Some(tx))
                    }
                }
            }
        };

        if is_leader {
            let tx = tx.unwrap();
            let status = fetch_func().await;
            self.flights.remove(&key);
            let final_status = if status { FlightStatus::Success } else { FlightStatus::Failed };
            let _ = tx.send(final_status);
            status
        } else {
            match rx.wait_for(|status| *status != FlightStatus::InFlight).await {
                Ok(status) => *status == FlightStatus::Success,
                Err(_) => false
            }
        }
    }  
}

pub struct Processor {
    upstream_mgr: Arc<UpstreamManager>,
    flight_group: Arc<FlightGroup>,
    db: Arc<ShardedLfuDb>
}

impl Processor {
    pub fn new(upstream_mgr: UpstreamManager, db: Arc<ShardedLfuDb>) -> Self {
        Self {
            upstream_mgr: Arc::new(upstream_mgr),
            db,
            flight_group: Arc::new(FlightGroup::new())
        }
    }

    pub async fn handle<'a>(&self, mut req_buf: BytesMut) -> Option<(Bytes, Arc<CachedTail>)> {
        // 提前强转为 usize 剥离指针特性，保证它是 Send 且不会在 parse_query 借用之后造成生命周期冲突
        let buf_ptr = req_buf.as_mut_ptr() as u64;
        let len = req_buf.len();
        let ParsedQuery{ q_len, cache_key, labels} = parse_query(&req_buf)?;
        
        if self.db.get(cache_key, q_len).await.is_none() {
            let flight_group = self.flight_group.clone();
            let db = self.db.clone();
            let upstream_mgr = self.upstream_mgr.clone();
            let is_ready = flight_group.wait_or_execute(cache_key, || async {
                let mut buf = unsafe {
                    from_raw_parts_mut(buf_ptr as *mut u8, len)
                };
                if let Some(upstream_resp) = upstream_mgr.dispatch(&mut buf, &labels).await {
                    let tail_start = HEADER_SIZE + q_len as usize;
                    if upstream_resp.len() < tail_start {
                        return false;
                    }

                    let cache_tail = CachedTail::new(upstream_resp, tail_start, q_len);
                    db.insert(cache_key, Arc::new(cache_tail)).await;
                    true
                } else {
                    false
                }
            }).await;
            
            if !is_ready {
                return None
            }
        }

        if let Some(cached_tail) = self.db.clone().get(cache_key, q_len).await {
            
            drop(labels);
            
            let req_len = req_buf.len();
            match build_header_slice(&mut req_buf, req_len, q_len, &cached_tail) {
                Ok(_) => {},
                Err(_) => {return None},
            };
            return Some((req_buf.freeze(), cached_tail.clone()))
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use bytes::BufMut;

    fn build_a_query(domain: &str) -> BytesMut {
        let mut req_buf = BytesMut::with_capacity(512);
        req_buf.extend_from_slice(&[
            0x12, 0x34, // 事务 ID: 0x1234
            0x01, 0x00, // Flags: 期望递归 (RD=1)
            0x00, 0x01, // QDCOUNT: 1
            0x00, 0x00, // ANCOUNT: 0
            0x00, 0x00, // NSCOUNT: 0
            0x00, 0x00, // ARCOUNT: 0
        ]);

        for label in domain.split('.') {
            req_buf.put_u8(label.len() as u8);
            req_buf.extend_from_slice(label.as_bytes());
        }
        req_buf.put_u8(0x00); // 域名结束标志

        req_buf.extend_from_slice(&[
            0x00, 0x01, // QTYPE: A (1)
            0x00, 0x01, // QCLASS: IN (1)
        ]);

        req_buf
    }

    #[tokio::test]
    async fn test_processor_real_network() {
        // 使用 Google 公共 DNS 发送真正的 DNS 网络请求
        let upstream_addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let upstream_mgr = UpstreamManager::new(upstream_addr);
        let db = Arc::new(ShardedLfuDb::new(100));
        let processor = Processor::new(upstream_mgr, db.clone());

        let req_buf = build_a_query("www.baidu.com");

        // 第一次请求：应该去往上游，并写入缓存
        let res1 = processor.handle(req_buf).await;
        assert!(res1.is_some(), "应当收到上游响应，请检查系统网络连接");
        let (req_header1, tail1) = res1.unwrap();
        assert!(tail1.answer_count > 0, "响应应当包含答案");
        assert_eq!(req_header1[0], 0x12, "客户端原始事务 ID 应保持一致");
        assert_eq!(req_header1[1], 0x34);

        // 第二次请求：应该直接命中缓存
        let mut req_buf2 = build_a_query("www.baidu.com");
        // 动态修改新请求的事务 ID，确保缓存复用时依然能重写正确的事务 ID 
        req_buf2[0] = 0xAA;
        req_buf2[1] = 0xBB;

        let res2 = processor.handle(req_buf2).await;
        println!("{:?}", req_header1);
        println!("{:?}", tail1.tail_bytes);
        assert!(res2.is_some(), "应当从缓存收到响应");
        let (req_header2, tail2) = res2.unwrap();
        
        assert_eq!(tail1.answer_count, tail2.answer_count, "缓存命中的记录应当与第一次一致");
        
        // 验证客户端是否把原始 ID 恢复了回来
        assert_eq!(req_header2[0], 0xAA);
        assert_eq!(req_header2[1], 0xBB);

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_processor_concurrent_flight() {
        // 使用 Google 公共 DNS
        let upstream_addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let upstream_mgr = UpstreamManager::new(upstream_addr);
        let db = Arc::new(ShardedLfuDb::new(1000));
        let processor = Arc::new(Processor::new(upstream_mgr, db));

        let mut handles = vec![];
        let num_tasks = 100;

        // 瞬间并发发起 100 个针对同一个域名的请求
        for i in 0..num_tasks {
            let proc_clone = processor.clone();
            handles.push(tokio::spawn(async move {
                let mut req_buf = build_a_query("github.com");
                let id_bytes = (i as u16).to_be_bytes();
                req_buf[0] = id_bytes[0];
                req_buf[1] = id_bytes[1];

                let res = proc_clone.handle(req_buf).await;
                (i, res)
            }));
        }

        for handle in handles {
            let (i, res) = handle.await.expect("Tokio 任务崩溃");
            assert!(res.is_some(), "任务 {} 未能获取到响应 (可能超时或出错)", i);
            
            let (req_header, tail) = res.unwrap();
            assert!(tail.answer_count > 0, "响应应当包含 Answer 记录");
            
            // 验证无论 FlightGroup 怎么合并请求，最终返回给每个客户端的事务 ID 必须是正确的
            let id_bytes = (i as u16).to_be_bytes();
            assert_eq!(req_header[0], id_bytes[0], "事务 ID 恢复错误发生串包");
            assert_eq!(req_header[1], id_bytes[1], "事务 ID 恢复错误发生串包");
        }
    }
}