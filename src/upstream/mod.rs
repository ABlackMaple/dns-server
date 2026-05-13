use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use tokio::sync::RwLock;

use crate::upstream::{client::UpstreamClient, trie::DomainRouter};

mod trie;
mod client;


pub struct UpstreamManager {
    router: DomainRouter,
    clients: RwLock<HashMap<SocketAddr, Arc<UpstreamClient>>>,
}

impl UpstreamManager {
    pub fn new(default_upstream: SocketAddr) -> Self {
        Self {
            router: DomainRouter::new(default_upstream),
            clients: RwLock::new(HashMap::new()),
        }
    }
    
    pub fn add_route(&mut self, domain: &str, upstream: SocketAddr) {
        self.router.insert_route(domain, upstream);
    }

    pub async fn get_client(&self, addr: SocketAddr) -> Option<Arc<UpstreamClient>> {
        {
            let map = self.clients.read().await;
            if let Some(client) = map.get(&addr) {
                return Some(client.clone());
            }
        }

        let mut map = self.clients.write().await;

        if let Some(client) = map.get(&addr) {
            return Some(client.clone());
        }
        
        if let Ok(client) = UpstreamClient::new(addr, 4, 200).await {
            let client_arc = Arc::new(client);
            map.insert(addr, client_arc.clone());
            return Some(client_arc);
        }
        None
    }

    pub async fn dispatch(&self, req_buf: &mut [u8], labels: &[&[u8]]) -> Option<Bytes> {
        let target_addr = self.router.find_upstream(labels);
        let client = self.get_client(target_addr).await?;

        client.ask(req_buf).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_real_network_dispatch_default() {
        // 使用 Google 公共 DNS 8.8.8.8:53
        let upstream_addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let manager = UpstreamManager::new(upstream_addr);

        // 手动构造一个查询 example.com 的标准 A 记录请求报文
        let mut req_buf = vec![
            0x12, 0x34, // 事务 ID: 0x1234
            0x01, 0x00, // Flags: 期望递归 (RD=1)
            0x00, 0x01, // QDCOUNT: 1
            0x00, 0x00, // ANCOUNT: 0
            0x00, 0x00, // NSCOUNT: 0
            0x00, 0x00, // ARCOUNT: 0
            0x07, b'e', b'x', b'a', b'm', b'p', b'l', b'e',
            0x03, b'c', b'o', b'm',
            0x00, // 域名结束标志
            0x00, 0x01, // QTYPE: A (1)
            0x00, 0x01, // QCLASS: IN (1)
        ];

        let labels: &[&[u8]] = &[b"example", b"com"];
        let res = manager.dispatch(&mut req_buf, labels).await;

        assert!(res.is_some(), "应当从公共 DNS 收到响应，请检查系统网络连接");
        let resp_bytes = res.unwrap();

        assert!(resp_bytes.len() >= 12, "响应报文至少包含 12 字节头部");
        assert_eq!(resp_bytes[0], 0x12, "客户端原始事务 ID 应保持一致");
        assert_eq!(resp_bytes[1], 0x34);

        // 验证标志位是否包含响应位 (QR = 1) -> 0x8000
        let flags = u16::from_be_bytes([resp_bytes[2], resp_bytes[3]]);
        assert_eq!(flags & 0x8000, 0x8000, "响应报文的 QR 标志位应当为 1");
    }

    #[tokio::test]
    async fn test_real_network_dispatch_custom_route() {
        let default_upstream: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let custom_upstream: SocketAddr = "1.1.1.1:53".parse().unwrap();
        
        let mut manager = UpstreamManager::new(default_upstream);
        // 为 cloudflare.com 配置专门的 1.1.1.1 路由
        manager.add_route("cloudflare.com", custom_upstream);
        
        // 构造查询 cloudflare.com A 记录报文
        let mut req_buf = vec![
            0xAA, 0xBB, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, b'c', b'l', b'o', b'u', b'd', b'f', b'l', b'a', b'r', b'e', 0x03, b'c', b'o', b'm', 0x00,
            0x00, 0x01, 0x00, 0x01,
        ];
        
        let labels: &[&[u8]] = &[b"cloudflare", b"com"];
        let res = manager.dispatch(&mut req_buf, labels).await;
        
        assert!(res.is_some(), "应当从 Cloudflare DNS 收到响应");
    }
}
