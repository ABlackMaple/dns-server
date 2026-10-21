# dns-server

使用 Rust 和 Tokio 编写的异步 UDP DNS 转发服务器，提供内存缓存、并发请求合并和基于域名后缀的上游路由能力。

> 项目仍在开发中。目前入口使用硬编码参数，不读取 `config.toml` 或 `hosts`，建议先在本地或受控网络中使用。

## 功能

- **UDP 转发**：将查询转发到上游 DNS，并恢复客户端事务 ID。
- **内存缓存**：分片存储响应，支持过期检查、近似 LFU 淘汰和访问热度衰减。
- **请求合并**：相同缓存键的并发未命中请求共享一次上游查询。
- **上游路由**：提供 `UpstreamManager::add_route` 接口，按域名后缀选择上游；默认入口尚未配置自定义规则。
- **并发收发**：使用 Tokio 多线程运行时、`SO_REUSEPORT` 和 `sendmsg` 分散写入。

## 环境要求

- 建议使用 Linux；当前实现依赖 Unix socket API，不支持直接在 Windows 上构建运行。
- 支持 Rust 2024 edition 的 Rust 工具链（Rust 1.85 或更新版本，建议使用最新 stable）。
- 可访问上游 DNS 的 UDP 53 端口。
- 可选：安装 `dig`，用于验证查询。

尚未安装 Rust 时，可参考 [Rust 官方安装说明](https://www.rust-lang.org/tools/install)。

## 快速开始

```bash
git clone https://github.com/ABlackMaple/dns-server.git
cd dns-server
cargo run --release
```

默认监听 `0.0.0.0:1053`，上游为 `1.1.1.1:53`。启动后，在另一个终端查询：

```bash
dig @127.0.0.1 -p 1053 example.com A +notcp
dig @127.0.0.1 -p 1053 example.com AAAA +notcp
```

按 `Ctrl+C` 退出进程。

也可以先构建，再运行二进制：

```bash
cargo build --release
./target/release/dns-server
```

> 默认监听所有 IPv4 网卡。仅供本机使用时，建议将监听地址改为 `127.0.0.1`；对外部署前应通过防火墙限制来源，避免成为开放 DNS 转发器。

## 配置

当前没有命令行参数或配置文件加载功能。修改参数后需要重新编译。

| 参数 | 当前值 | 修改位置 |
| --- | --- | --- |
| 监听端口 | `1053` | `src/bin/server.rs` 中的 `PORT` |
| 监听地址 | `0.0.0.0` | `src/bin/server.rs` 中的 `listen_addr` |
| 默认上游 | `1.1.1.1:53` | `src/bin/server.rs` 中的 `upstream_addr` |
| 工作线程数 | `20` | `src/bin/server.rs` 中的 `worker_threads` |
| 缓存容量参数 | `65536 * 1024` | `src/bin/server.rs` 中的 `ShardedLfuDb::new(...)` |
| 每个上游的 socket 数 | `4` | `src/upstream/mod.rs` 中的 `UpstreamClient::new(...)` |
| 每个上游的并发上限 | `200` | 同上 |
| 上游响应等待超时 | `3 秒` | `src/upstream/client.rs` 中的 `timeout(...)` |

缓存容量参数表示条目数量，而不是字节数。默认值较大，启动时会预分配相关容器，资源有限时建议调小。

要添加域名路由，可在创建 `Processor` 前修改上游管理器：

```rust
let mut upstream_manager = UpstreamManager::new(upstream_addr);
upstream_manager.add_route("example.com", "8.8.8.8:53".parse()?);
```

## 请求流程

```text
客户端 UDP 查询
    ↓
解析请求 → 查询缓存 ── 命中 ──→ 组装响应并返回
                ↓ 未命中
          合并相同键的并发请求
                ↓
          根据域名选择上游
                ↓
          UDP 转发 → 写入缓存 → 返回客户端
```

## 项目结构

```text
src/
├── bin/server.rs     # 程序入口与默认参数
├── server.rs         # UDP 监听与响应发送
├── processor.rs      # 请求处理、缓存查询与并发请求合并
├── dns/              # DNS 报文解析与响应组装
├── cache/            # 响应缓存、分片 LFU 存储与清理逻辑
├── upstream/         # 上游连接、请求分发与域名路由
├── shutdown.rs       # 关闭通知辅助类型
└── lib.rs            # 库入口
```

## 测试

```bash
cargo test
```

注意：完整测试集包含真实公网 DNS 请求和一次创建 **600,000 个任务**的并发测试，可能受网络限制影响，并消耗较多时间和内存。

如需先运行轻量的本地上游通信测试：

```bash
cargo test test_ask_success
cargo test test_ask_timeout
```

## 当前限制

- 仅实现 UDP 传输，不支持 TCP DNS、DoT 或 DoH。
- 客户端收发路径目前仅支持 IPv4；查询 AAAA 记录不等同于支持 IPv6 传输。
- 客户端和上游的单次接收缓冲区均为 4096 字节，较大的报文可能被截断，且没有 TCP 回退。
- 上游失败或超时时，当前处理路径可能不返回响应，客户端会等待超时。
- 缓存采用自定义过期策略，返回缓存响应时不会逐条递减记录中的 TTL。
- `config.toml` 和 hosts 加载尚未接入启动流程。
