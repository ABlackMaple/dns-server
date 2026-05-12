use std::{collections::HashMap, process::id, sync::{Arc, RwLock, atomic::Ordering}, time::Instant};

use rand::{RngExt, rngs::ThreadRng, seq::index};

use crate::cache::{CacheStore, CachedTail};

struct State {
    map: HashMap<u64, (Arc<CachedTail>, usize)>,
    keys: Vec<u64>,
    sample_buffer: Vec<usize>
}

impl State {
    fn new(max_size: usize) -> Self {
        State { map: HashMap::with_capacity(max_size), keys: Vec::with_capacity(max_size), sample_buffer: Vec::with_capacity(20)}
    }

    fn rand_choose_multiple(&mut self, rng: &mut ThreadRng, cnt: usize) -> &Vec<usize> {
        let len = self.map.len();
        let cnt = cnt.min(len);
        self.sample_buffer.clear();
        for i in 0..cnt {
            let idx = rng.random_range(..(len - i));
            let target = self.keys.len() - 1 - i;
            if idx != target {
                self.keys.swap(idx, target);
                let key_a = self.keys[idx];
                let key_b = self.keys[target];
                if let Some((_, map_idx)) = self.map.get_mut(&key_a) {
                    *map_idx = idx;
                }
                if let Some((_, map_idx)) = self.map.get_mut(&key_b) {
                    *map_idx = target;
                }
            }
            self.sample_buffer.push(target);
        }

        &self.sample_buffer
    }
}


struct CacheShard {
    data: RwLock<State>,
    max_size: usize,
}

impl CacheShard {
    fn new(max_size: usize) -> Self{
        Self {
            data: RwLock::new(State::new(max_size)),
            max_size
        }
    }
    fn remove_by_index(index: usize, data: &mut State) {
        let last_index = data.keys.len() - 1;
        if index != last_index {
            data.keys.swap(index, last_index);
            let swapped_key = data.keys[index];
            if let Some((_, map_idx)) = data.map.get_mut(&swapped_key) {
                *map_idx = index;
            }
        }
        Self::pop(data);
    }

    fn pop(data: &mut State) {
        if !data.keys.is_empty() {
            let key = data.keys.pop().unwrap();
            data.map.remove(&key);
        }
    }

    fn remove(key: &u64, data: &mut State) {
        let idx = if let Some((_, idx)) = data.map.get(key) {
            idx
        } else {
            return;
        };
        Self::remove_by_index(*idx, data);
    }

    fn evict_approximate_lfu(&self, data: &mut State) {
        let mut rng = rand::rng();
        let _ = data.rand_choose_multiple(&mut rng, 5);

        let mut min_hits = usize::MAX;
        let mut victim_idx = None;
        for &idx in &data.sample_buffer {
            if let Some((entry, _)) = data.map.get(&data.keys[idx]) {
                let hits = entry.hits.load(Ordering::Relaxed);
                if hits < min_hits {
                    min_hits = hits;
                    victim_idx = Some(idx);
                }
            }
        }

        if let Some(idx) = victim_idx {
            Self::remove_by_index(idx, data);
        }
    }

    fn random_evict_expired(&self) -> usize {
        let mut data = match self.data.try_write() {
            Ok(guard) => guard,
            Err(_) => return 0
        };

        let mut rng = rand::rng();
        let mut total_removed = 0;
        let now = Instant::now();
        let sample_size = 20;

        let mut wait_to_remove = [0; 20];
        loop {
            let _ = data.rand_choose_multiple(&mut rng, sample_size);
            if data.sample_buffer.is_empty() {
                break;
            }

            let mut expired_in_sample = 0;
            for &idx in &data.sample_buffer {
                if let Some((entry, _)) = data.map.get(&data.keys[idx]) {
                    if now > entry.expires_at {
                        wait_to_remove[expired_in_sample] = idx;
                        expired_in_sample += 1;
                    }
                }
            }

            for idx in 0..expired_in_sample {
                Self::remove_by_index(wait_to_remove[idx], &mut data);
            }
            total_removed += expired_in_sample;
            if expired_in_sample < sample_size / 4 {
                break;
            }
        }


        total_removed
    }

}


const SHARD_COUNT: usize = 64;

pub struct ShardedLfuDb {
    shareds: Vec<CacheShard>,
}

impl ShardedLfuDb {
    pub fn new(total_max_size: usize) -> Self {
        let size_per_shard = (total_max_size + SHARD_COUNT - 1) / SHARD_COUNT;
        let size_per_shard = size_per_shard.max(1);
        let mut shards = Self {
            shareds: Vec::with_capacity(SHARD_COUNT)
        };
        for _ in 0..SHARD_COUNT {
            shards.shareds.push(CacheShard::new(size_per_shard));
        }
        shards
    }

    fn get_shard_index(&self, key: u64) -> usize {
        (key as usize) % SHARD_COUNT
    }
}

impl CacheStore for ShardedLfuDb {
    async fn get(&self, key: u64, q_len: u16) -> Option<Arc<CachedTail>> {
        let shard_idx = self.get_shard_index(key);

        let shard = &self.shareds[shard_idx];

        let (is_expired, valid_entry) = {
            let cache = shard.data.read().unwrap();
            if let Some((entry, _)) = cache.map.get(&key) {
                if entry.q_len != q_len {
                    return None;
                }
                if Instant::now() > entry.expires_at {
                    (true, None)
                } else {
                    entry.record_access();
                    (false, Some(entry.clone()))
                }
            } else {
                return None
            }
        };

        if is_expired {
            if let Ok(mut data) = shard.data.try_write() {
                if let Some((entry, idx)) = data.map.get(&key) {
                    if Instant::now() > entry.expires_at {
                        CacheShard::remove_by_index(*idx, &mut data);
                    }
                }
            }
        }
        
        valid_entry
    }

    async fn insert(&self, key: u64, tail: Arc<CachedTail>) {
        let shard_idx = self.get_shard_index(key);
        let shard = &self.shareds[shard_idx];
        
        let mut data = shard.data.write().unwrap();

        if let Some((_, existing_idx)) = data.map.get(&key) {
            let idx = *existing_idx;
            data.map.insert(key, (tail, idx));
        } else {
            if data.map.len() >= shard.max_size {
                shard.evict_approximate_lfu(&mut data);
            }
            let idx = data.keys.len();
            data.keys.push(key);
            data.map.insert(key, (tail, idx));
        }

    }

    async fn len(&self) -> usize {
        self.shareds.iter().map(|s| s.data.read().unwrap().map.len()).sum()
    }

    async fn purge_expired(&self) -> usize {
        let mut total = 0;
        for shard in &self.shareds {
            total += shard.random_evict_expired();
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, AtomicU64};
    use std::time::Duration;
    use bytes::Bytes;

    fn dummy_tail(expires_in_secs: i64, hits: usize) -> Arc<CachedTail> {
        let expires_at = if expires_in_secs >= 0 {
            Instant::now() + Duration::from_secs(expires_in_secs as u64)
        } else {
            Instant::now() - Duration::from_secs(expires_in_secs.abs() as u64)
        };

        Arc::new(CachedTail {
            tail_bytes: Bytes::new(),
            // ttl_offsets: vec![],
            expires_at,
            answer_count: 0,
            authority_count: 0,
            additional_count: 0,
            q_len: 0,
            hits: AtomicUsize::new(hits),
            last_accessed_sec: AtomicU64::new(0),
        })
    }

    #[test]
    fn test_rand_choose_multiple_updates_map() {
        let mut state = State::new(10);
        for i in 0..5 {
            let key = i as u64;
            state.keys.push(key);
            state.map.insert(key, (dummy_tail(10, 0), i));
        }

        let mut rng = rand::rng();
        state.rand_choose_multiple(&mut rng, 3);

        // 验证由于随机交换，Map 中的下标是否跟上了 keys 数组的变化
        for (i, key) in state.keys.iter().enumerate() {
            assert_eq!(state.map.get(key).unwrap().1, i);
        }
    }

    #[test]
    fn test_remove_by_index_updates_map() {
        let mut state = State::new(10);
        for i in 0..3 {
            let key = i as u64;
            state.keys.push(key);
            state.map.insert(key, (dummy_tail(10, 0), i));
        }

        // 移除中间的元素（key=1, idx=1），那么 key=2 将会被交换到 idx=1 的位置
        CacheShard::remove_by_index(1, &mut state);

        assert_eq!(state.keys.len(), 2);
        assert_eq!(state.keys[0], 0);
        assert_eq!(state.keys[1], 2);

        assert_eq!(state.map.get(&0).unwrap().1, 0);
        assert_eq!(state.map.get(&2).unwrap().1, 1);
        assert!(state.map.get(&1).is_none());
    }

    #[test]
    fn test_random_evict_expired() {
        let shard = CacheShard::new(40);
        {
            let mut data = shard.data.write().unwrap();
            // 塞入 15 个过期僵尸记录
            for i in 0..15 {
                let key = i as u64;
                data.keys.push(key);
                let idx = data.keys.len() - 1;
                data.map.insert(key, (dummy_tail(-10, 0), idx));
            }
            // 塞入 15 个健康记录
            for i in 15..30 {
                let key = i as u64;
                data.keys.push(key);
                let idx = data.keys.len() - 1;
                data.map.insert(key, (dummy_tail(100, 0), idx));
            }
        }

        let removed = shard.random_evict_expired();
        assert!(removed > 0, "应当成功清理了过期条目");

        let data = shard.data.read().unwrap();
        for i in 15..30 {
            assert!(data.map.contains_key(&i), "健康条目 {} 不应被驱逐", i);
        }
    }

    #[tokio::test]
    async fn test_sharded_db_get_and_insert() {
        let db = ShardedLfuDb::new(100);
        
        // 插入有效记录，验证命中是否可用
        db.insert(100, dummy_tail(100, 0)).await;
        let res = db.get(100, 0).await;
        assert!(res.is_some(), "应当返回未过期的条目");

        // 插入过期记录，验证是否拦截
        db.insert(200, dummy_tail(-10, 0)).await;
        let res = db.get(200, 0).await;
        assert!(res.is_none(), "已过期的条目应当返回 None");
        
        // 验证 Get 拦截时是否顺便触发了删除
        let len = db.len().await;
        assert_eq!(len, 1, "过期的条目应该被清理出去了");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_sharded_db_heavy_io() {
        // 设置一个更大的容量
        const MAX_CAPACITY: usize = 500_000;
        let db = Arc::new(ShardedLfuDb::new(MAX_CAPACITY));
        let mut handles = vec![];

        const WRITER_TASKS: u64 = 64;
        const READER_TASKS: u64 = 64;
        const ITEMS_PER_TASK: u64 = 100_000;

        // 启动 16 个并发写入任务，总共写入 16 * 250k = 4,000,000 条数据
        for i in 0..WRITER_TASKS {
            let db_clone = db.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..ITEMS_PER_TASK {
                    let key = i * ITEMS_PER_TASK + j;
                    // 一半的数据是 100 秒后过期，一半的数据是一插入就过期
                    let expire = if j % 2 == 0 { 100 } else { -10 };
                    db_clone.insert(key, dummy_tail(expire, 0)).await;
                }
            }));
        }

        // 启动 16 个并发读取任务
        for i in 0..READER_TASKS {
            let db_clone = db.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..ITEMS_PER_TASK {
                    let key = i * ITEMS_PER_TASK + j;
                    let _ = db_clone.get(key, 0).await;
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let final_len = db.len().await;
        assert!(final_len <= MAX_CAPACITY + SHARD_COUNT, "缓存应该被正确截断，当前大小：{}", final_len);
        
        let purged = db.purge_expired().await;
        assert!(purged > 0, "后台应当成功清理残留的过期条目");
    }
}
