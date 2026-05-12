use tokio::sync::broadcast;

pub struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Shutdown { is_shutdown: false, notify }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}