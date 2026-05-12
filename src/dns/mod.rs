pub mod types;
pub mod parser;
pub mod wire;


use smallvec::SmallVec;
pub use types::*;
// pub use wire::build_cached_response;

pub struct ParsedQuery<'a> {
    pub cache_key: u64,
    pub q_len: u16,
    pub labels: SmallVec<[&'a [u8]; 8]>
}
// impl<'a> Drop for ParsedQuery<'a> {
//     fn drop(&mut self) {
//         self.labels.
//     }
// }