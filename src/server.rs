use std::{io::IoSlice, net::SocketAddr, os::fd::AsRawFd, sync::Arc};

use bytes::BytesMut;
use nix::sys::socket::{MsgFlags, SockaddrIn, sendmsg};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::processor::Processor;



pub struct Server {
    processor: Arc<Processor>,
    listen_addr: SocketAddr,
    worker_threads: usize,
}

impl Server {
    pub fn new(processor: Arc<Processor>, listen_addr: SocketAddr, worker_threads: usize) -> Self {
        Self { processor, listen_addr, worker_threads }
    }

    async fn send_to_vectored(socket: &UdpSocket, target: &SockaddrIn, iov: &[IoSlice<'_>]) -> anyhow::Result<usize> {
        loop {
            socket.writable().await?;
            
            match socket.try_io(tokio::io::Interest::WRITABLE, || {
                sendmsg(socket.as_raw_fd(), iov, &[], MsgFlags::empty(), Some(target))
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32)) 
            }) {
                Ok(result)  => return Ok(result),
                Err(_would_block) => continue,
            }
            
        }
    }

    pub async fn run(&self) -> anyhow::Result<()>{
        let mut handles = vec![];

        for _ in 0..self.worker_threads {
            let processor = self.processor.clone();
            let listen_addr = self.listen_addr;

            let handle = tokio::spawn(async move {
                let domain = if listen_addr.is_ipv4() {
                    Domain::IPV4
                } else {
                    Domain::IPV6
                };
                let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).unwrap();

                socket.set_reuse_port(true).unwrap();
                socket.set_nonblocking(true).unwrap();

                let _ = socket.set_recv_buffer_size(8 * 1024 * 1024);
                let _ = socket.set_send_buffer_size(8 * 1024 * 1024);

                socket.bind(&listen_addr.into()).unwrap();

                let tokio_socket = Arc::new(UdpSocket::from_std(socket.into()).unwrap());


                let mut buf = BytesMut::with_capacity(64 * 1024);

                loop {
                    if buf.capacity() < 4096 {
                        buf.reserve(64 * 1024);
                    }
                    unsafe {
                        buf.set_len(4096);
                    }
                    match tokio_socket.recv_from(&mut buf).await {
                        Ok((len, client_addr)) => {
                            unsafe {
                                buf.set_len(len);
                                let req_data = buf.split();
                                let proc = processor.clone();
                                let sock = tokio_socket.clone();

                                tokio::spawn(async move {
                                    if let Some(io_resp) = proc.handle(req_data).await {
                                        let iov = [IoSlice::new(&io_resp.0), IoSlice::new(&io_resp.1.tail_bytes)];
                                        let client_addr = match client_addr {
                                            SocketAddr::V4(v4) => SockaddrIn::from(v4),
                                            SocketAddr::V6(_) => { panic!("Ipv6 is not supported now")}
                                        };
                                        if let Err(e) = Self::send_to_vectored(&sock, &client_addr, &iov).await {
                                            eprint!("Send to vectored error: {}", e);
                                        };
                                    }
                                });
                            }
                        },
                        Err(e) => {
                            eprintln!("Revc Failed: {}", e)
                        }
                    }
                }
            });

            handles.push(handle);

            
        }


        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::fast_db::ShardedLfuDb, processor::Processor, upstream::UpstreamManager,
    };
    use bytes::BufMut;
    use std::time::Duration;

    fn build_a_query(domain: &str) -> BytesMut {
        let mut req_buf = BytesMut::with_capacity(512);
        req_buf.extend_from_slice(&[
            0x00, 0x00, // 事务 ID (将在每个请求中设置)
            0x01, 0x00, // Flags: 标准查询 (期望递归)
            0x00, 0x01, // Questions: 1
            0x00, 0x00, // Answer RRs: 0
            0x00, 0x00, // Authority RRs: 0
            0x00, 0x00, // Additional RRs: 0
        ]);

        for label in domain.split('.') {
            req_buf.put_u8(label.len() as u8);
            req_buf.extend_from_slice(label.as_bytes());
        }
        req_buf.put_u8(0x00); // 域名末尾

        req_buf.extend_from_slice(&[
            0x00, 0x01, // QTYPE: A
            0x00, 0x01, // QCLASS: IN
        ]);

        req_buf
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_server_concurrent_requests() {
        // 1. 设置服务器组件
        let upstream_addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let upstream_mgr = UpstreamManager::new(upstream_addr);
        let db = Arc::new(ShardedLfuDb::new(1000));
        let processor = Arc::new(Processor::new(upstream_mgr, db));

        // 2. 绑定到随机端口以避免测试冲突
        let listener_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let actual_listen_addr = listener_socket.local_addr().unwrap();
        drop(listener_socket); // 释放端口, SO_REUSEPORT 会允许重新绑定

        let server = Server::new(processor, actual_listen_addr, 2);

        // 3. 在后台任务中运行服务器
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                eprintln!("服务器运行失败: {}", e);
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await; // 等待服务器启动

        // 4. 并发启动客户端发送请求
        let mut handles = vec![];
        for i in 0..100 {
            handles.push(tokio::spawn(async move {
                let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                client_socket.connect(actual_listen_addr).await.unwrap();

                let mut query = build_a_query("google.com");
                let id_bytes = (i as u16).to_be_bytes();
                query[0..2].copy_from_slice(&id_bytes);

                client_socket.send(&query).await.unwrap();

                let mut resp_buf = [0u8; 512];
                let (len, _) = tokio::time::timeout(Duration::from_secs(5), client_socket.recv_from(&mut resp_buf)).await.expect("请求超时").unwrap();
                
                let resp = &resp_buf[..len];
                println!("Request {} received response ({} bytes): {:02X?}", i, resp.len(), resp);

                assert!(resp.len() >= 12, "响应报文过短 (请求 {})", i);
                assert_eq!(&resp[0..2], &id_bytes, "事务 ID 不匹配 (请求 {})", i);
                assert!(u16::from_be_bytes([resp[6], resp[7]]) > 0, "响应中应包含答案 (请求 {})", i);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}