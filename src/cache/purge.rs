use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::interval};

use crate::{cache::{CacheStore, fast_db::ShardedLfuDb}, shutdown::Shutdown};


pub async fn run_purge_task(db: Arc<ShardedLfuDb>, mut shutdown: Shutdown, _shutdown_complete: mpsc::Sender<()>) {
    let mut interval = interval(Duration::from_secs(1));

    loop {

        tokio::select! {
            _ = interval.tick() => {
                let cleaned = db.purge_expired().await;
                if cleaned > 0 {
                    
                }

            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
    
}