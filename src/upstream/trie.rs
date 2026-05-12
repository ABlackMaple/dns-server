use std::collections::HashMap;

use std::net::SocketAddr;

#[derive(Default)]
pub struct RouteNode {
    children: HashMap<Vec<u8>, RouteNode>,
    pub upstream: Option<SocketAddr>
}

pub struct DomainRouter {
    root: RouteNode,
    default_upstream: SocketAddr,
}

impl DomainRouter {
    pub fn new(default_upstream: SocketAddr) -> Self {
        Self {
            root: RouteNode::default(),
            default_upstream,
        }
    }

    pub fn insert_route(&mut self, domain: &str, upstream: SocketAddr) {
        let labels: Vec<&str> = domain.split('.').filter(|s| !s.is_empty()).collect();
        let mut current = &mut self.root;

        for label in labels.into_iter().rev() {
            let label_bytes = label.to_lowercase().into_bytes();
            current = current.children.entry(label_bytes).or_insert_with(RouteNode::default);
        }
        current.upstream = Some(upstream);
    }

    pub fn find_upstream(&self, labels: &[&[u8]]) -> SocketAddr {
        if labels.is_empty() {
            return self.default_upstream.clone();
        }

        let mut current = &self.root;
        let mut best_match = &self.default_upstream;

        for label in labels.iter().rev() {
            let mut lower_buf = [0u8; 64];
            let len = label.len().min(63);

            lower_buf[..len].copy_from_slice(&label[..len]);
            lower_buf[..len].make_ascii_lowercase();

            if let Some(child) = current.children.get(&lower_buf[..len]) {
                current = child;
                if let Some(up) = &current.upstream {
                    best_match = up;
                }
            } else {
                break;
            }
        }

        best_match.clone()
    }
}