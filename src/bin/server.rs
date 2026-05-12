use dns_server::{cache::fast_db::ShardedLfuDb, processor::Processor, server::Server, upstream::UpstreamManager};


pub const PORT:u16 = 1053;

// #[tokio::main]
// async fn main() -> Result<()> {
//     let config = init();

//     if let Err(e) = run(config) {
//         todo!()
//     }

//     Ok(())
// }

// fn init() -> Config{
//     todo!();
// }

// fn run(config: Config) -> Result<()> {
    
    
//     todo!()
// }

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Builder;



fn main() -> anyhow::Result<()> {
    // tracing_subscriber::fmt::init();

    let listen_addr: SocketAddr = format!("0.0.0.0:{}", PORT).parse()?;
    let upstream_addr: SocketAddr = "1.1.1.1:53".parse()?;
    let worker_threads = 20; 

    let rt = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("dns-worker")
        .build()?;

    rt.block_on(async {
        
        let db = Arc::new(ShardedLfuDb::new(65536 * 1024));

        let upstream_manager = UpstreamManager::new(upstream_addr);

        let processor = Arc::new(Processor::new(upstream_manager, db));

        let server = Server::new(processor, listen_addr, worker_threads);

        println!("🎧 正在监听端口: {}", listen_addr);

        // 6. 运行并处理优雅退出
        // 监听 Ctrl+C 或其他终止信号
        tokio::select! {
            res = server.run() => {
                if let Err(e) = res {
                    eprintln!("❌ 服务器运行异常: {}", e);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n🛑 接收到退出信号，正在安全关闭...");
            }
        }

        Ok(())
    })
}