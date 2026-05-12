#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOGAL: Jemalloc = Jemalloc;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub mod server;
pub mod dns;
pub mod cache;
pub mod shutdown;
pub mod processor;
pub mod upstream;