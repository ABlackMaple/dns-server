use std::future::pending;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::{net::SocketAddr, sync::mpsc::channel};
use std::sync::Arc;

use bytes::{Bytes, BytesMut, BufMut};
use dashmap::DashMap;
use smallvec::SmallVec;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::Semaphore;
use tokio::{net::UdpSocket, sync::{mpsc, oneshot}};

use crate::{Result, dns::HEADER_SIZE};


pub struct UpstreamClient {
    pending: Arc<DashMap<u16, oneshot::Sender<BytesMut>>>,
    next_id: AtomicU16,
    next_index: AtomicUsize,
    sockets: Vec<Arc<UdpSocket>>,
    semaphore: Arc<Semaphore>,
}


impl UpstreamClient {
    pub async fn new(target_addr: SocketAddr, socket_count: usize, max_concurrent: usize) -> Result<Self> {
        let mut sockets = Vec::with_capacity(socket_count);
        let domain = match target_addr {
            SocketAddr::V4(_) => Domain::IPV4,
            SocketAddr::V6(_) => Domain::IPV6,
        };
        let pending: Arc<DashMap<u16, oneshot::Sender<BytesMut>>> = Arc::new(DashMap::with_capacity(2048));
        
        for _i in 0..socket_count {

            let raw_socket = Socket::new(domain.clone(), Type::DGRAM, Some(Protocol::UDP))?;
    
            let _ = raw_socket.set_recv_buffer_size(8 * 1024 * 1024);
            let _ = raw_socket.set_send_buffer_size(8 * 1024 * 1024);
    
            raw_socket.set_nonblocking(true)?;
    
            let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            raw_socket.bind(&bind_addr.into())?;
    
            let std_socket: std::net::UdpSocket = raw_socket.into();
            let socket = Arc::new(tokio::net::UdpSocket::from_std(std_socket)?);
            sockets.push(socket.clone());
            
            socket.connect(target_addr).await?;
    
            let sock_recv = socket.clone();
            let pending_recv = pending.clone();
            tokio::spawn(async move {
                let mut chunk = BytesMut::with_capacity(64 * 1024);
    
                loop {
                    if chunk.capacity() < 4096 {
                        chunk.reserve(64 * 1024);
                    }
                    unsafe { chunk.set_len(4096) };
                    if let Ok(size) = sock_recv.recv(&mut chunk).await {
                        unsafe { chunk.set_len(size); }
                        if size < HEADER_SIZE {
                            chunk.clear();
                            continue;
                        }
                        let packet = chunk.split_to(size);
                        let internal_id = u16::from_be_bytes([packet[0], packet[1]]);
    
                        if let Some((_, sender)) = pending_recv.remove(&internal_id) {
                            let _ = sender.send(packet);
                        }
                    }
                }
            });

        }    
        Ok(Self { 
            pending, 
            next_id: AtomicU16::new(0),
            sockets,
            next_index: AtomicUsize::new(0),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        })

    }

    pub async  fn ask(&self, req_buf: &mut [u8]) -> Option<Bytes> {
        if req_buf.len() < HEADER_SIZE {
            return None;
        }  
        let _permit = self.semaphore.acquire().await.ok()?;
        
        let origin_id = [req_buf[0], req_buf[1]];
        let internal_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.insert(internal_id, resp_tx);

        let id_bytes = internal_id.to_be_bytes();
        req_buf[0] = id_bytes[0];
        req_buf[1] = id_bytes[1];
        
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % self.sockets.len();
        let socket = self.sockets[idx].clone();

        if socket.send(req_buf).await.is_err() {
            self.pending.remove(&internal_id);
            return None;
        }


        match tokio::time::timeout(std::time::Duration::from_secs(3), resp_rx).await {
            Ok(Ok(mut resp_bytes)) => {
                resp_bytes[0] = origin_id[0];
                resp_bytes[1] = origin_id[1];
                req_buf[0] = origin_id[0];
                req_buf[1] = origin_id[1];
                Some(resp_bytes.freeze())
            }
            _ => {
                self.pending.remove(&internal_id);
                None
            }
        }
    }
}


#[cfg(test)]
mod tests {
use socket2::{Domain, Protocol, Socket, Type};

use super::*;
    use std::time::{Duration, Instant};

    async fn echo_server() -> (SocketAddr, oneshot::Sender<()>) {
        let domain = Domain::IPV4;
        let raw_socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).unwrap();

        // 给靶子也穿上 8MB 的防弹衣
        let _ = raw_socket.set_recv_buffer_size(80 * 1024 * 1024);
        let _ = raw_socket.set_send_buffer_size(80 * 1024 * 1024);
        raw_socket.set_nonblocking(true).unwrap();
        
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        raw_socket.bind(&bind_addr.into()).unwrap();

        let std_socket: std::net::UdpSocket = raw_socket.into();
        let socket = Arc::new(tokio::net::UdpSocket::from_std(std_socket).unwrap());
        
        let addr = socket.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        let socket_tx = socket.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024]; // 稍微调大一点，兼容大包测试
            loop {
                tokio::select! {
                    Ok((len, src)) = socket.recv_from(&mut buf) => {
                        if len >= HEADER_SIZE {
                            let socket_tx = socket_tx.clone();
                            let data = buf[..len].to_vec(); // 必须拷贝出来，防止多线程竞争
                            
                            tokio::spawn(async move {
                                if let Err(e) = socket_tx.send_to(&data, src).await {
                                    // 【核心修复】：不要 unwrap！满了就打日志，不至于让任务崩溃
                                    println!("Echo Failed: {}", e);
                                }
                            });
                        }
                    }
                    _ = &mut rx => break,
                }
            }
        });

        (addr, tx)
    }

    /// 启动一个黑洞服务器，接收请求但不回复，用于触发超时
    async fn blackhole_server() -> (SocketAddr, oneshot::Sender<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, mut rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                tokio::select! {
                    _ = socket.recv_from(&mut buf) => { /* 丢弃，不回包 */ }
                    _ = &mut rx => break,
                }
            }
        });

        (addr, tx)
    }

    #[tokio::test]
    async fn test_ask_success() {
        let (addr, _shutdown) = echo_server().await;
        let client = UpstreamClient::new(addr, 2, 10).await.expect("Client 创建失败");

        // 伪造 DNS 请求（必须大于等于 HEADER_SIZE = 12 字节）
        let mut req_buf = vec![0u8; 12];
        req_buf[0] = 0x12; // 原始 ID
        req_buf[1] = 0x34;

        let resp = client.ask(&mut req_buf).await.expect("应当成功收到回应");
        assert_eq!(resp.len(), 12);
        // 验证客户端是否把原始 ID 恢复了回来
        assert_eq!(resp[0], 0x12);
        assert_eq!(resp[1], 0x34);
    }

    #[tokio::test]
    async fn test_ask_timeout() {
        let (addr, _shutdown) = blackhole_server().await;
        let client = UpstreamClient::new(addr, 2, 10).await.expect("Client 创建失败");

        let mut req_buf = vec![0u8; 12];
        
        let start = Instant::now();
        let resp = client.ask(&mut req_buf).await;
        let elapsed = start.elapsed();

        assert!(resp.is_none(), "黑洞服务器不应返回数据");
        assert!(elapsed >= Duration::from_secs(3), "应当完整等待 3 秒超时时间");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn test_ask_concurrent() {
    let (addr, _shutdown) = echo_server().await;
    let client = Arc::new(UpstreamClient::new(addr, 8, 100).await.unwrap());

    // 【新增】：使用信号量控制瞬间的并发洪峰，防止打爆操作系统 UDP 缓冲区！
    // 允许同时有 5000 个请求在路上，这已经是非常恐怖的高并发量了

    let mut handles = vec![];
    
    // 总量保持 60,000 压测不变
    for i in 0..600000u32 {
        let client_clone = client.clone();

        handles.push(tokio::spawn(async move {
            // 将 permit 的生命周期绑定到这个协程中，跑完自动释放
            
            let mut req_buf = vec![0u8; 12];
            
            // 【修复 Bug】：将 u32 强转为 u16，取真正的末尾两字节作为验证 ID！
            let id_bytes = (i as u16).to_be_bytes();
            req_buf[0] = id_bytes[0];
            req_buf[1] = id_bytes[1];

            // 发起请求并等待
            let resp = client_clone.ask(&mut req_buf).await.expect("应当收到响应 (如果报错说明超时丢包)");
            
            assert_eq!(resp[0], id_bytes[0], "报文 ID 不匹配，发生串包！");
            assert_eq!(resp[1], id_bytes[1]);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
}