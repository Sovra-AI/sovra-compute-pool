//! Content-addressed blob store: every file is named by the BLAKE3 hash of its content, so
//! it is immutable, self-verifying and can be fetched from any peer that has it.
//!
//! * `blobs/`  — cache copies, evictable LRU (mtime is the last-use clock)
//! * `kept/`   — blobs this node originated (uploads, task outputs); never evicted
//! * `tmp/`    — in-flight writes; a blob only appears under its hash after verification

use std::collections::{HashMap, HashSet};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::io::AsyncWriteExt;

use crate::proto::{Hash, is_hash};

pub struct Store {
    blobs: PathBuf,
    kept: PathBuf,
    tmp: PathBuf,
    /// Hashes in use by tasks on this node; exempt from eviction.
    pins: Mutex<HashMap<Hash, usize>>,
    /// Bumped whenever the set of stored blobs changes.
    version: AtomicU64,
    tmp_seq: AtomicU64,
    fetching: tokio::sync::Mutex<HashMap<Hash, Arc<tokio::sync::Mutex<()>>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct GcPolicy {
    /// Cap on evictable cache bytes.
    pub max_bytes: Option<u64>,
    /// Keep at least this much of the filesystem free.
    pub min_free_bytes: u64,
    /// Never evict blobs used more recently than this.
    pub min_age: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    pub hash: Hash,
    pub size: u64,
    pub mtime: SystemTime,
}

impl Store {
    pub fn open(root: &Path) -> io::Result<Self> {
        let store = Self {
            blobs: root.join("blobs"),
            kept: root.join("kept"),
            tmp: root.join("tmp"),
            pins: Mutex::default(),
            version: AtomicU64::new(1),
            tmp_seq: AtomicU64::new(0),
            fetching: tokio::sync::Mutex::default(),
        };
        // Anything left in tmp/ is a write that never completed.
        let _ = std::fs::remove_dir_all(&store.tmp);
        for dir in [&store.blobs, &store.kept, &store.tmp] {
            std::fs::create_dir_all(dir)?;
        }
        Ok(store)
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Local path of a blob, if present. Marks cache copies as just used.
    pub fn find(&self, hash: &str) -> Option<PathBuf> {
        if !is_hash(hash) {
            return None;
        }
        let kept = self.kept.join(hash);
        if kept.is_file() {
            return Some(kept);
        }
        let cached = self.blobs.join(hash);
        if !cached.is_file() {
            return None;
        }
        if let Ok(file) = std::fs::File::options().write(true).open(&cached) {
            let _ = file.set_modified(SystemTime::now());
        }
        Some(cached)
    }

    pub fn list(&self) -> Vec<(Hash, u64)> {
        let mut all = Vec::new();
        for dir in [&self.kept, &self.blobs] {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                if let (Ok(name), Ok(meta)) = (entry.file_name().into_string(), entry.metadata())
                    && is_hash(&name)
                {
                    all.push((name, meta.len()));
                }
            }
        }
        all
    }

    pub fn pin(&self, hash: &str) {
        *crate::lock(&self.pins).entry(hash.to_owned()).or_default() += 1;
    }

    pub fn unpin(&self, hash: &str) {
        let mut pins = crate::lock(&self.pins);
        if let Some(count) = pins.get_mut(hash) {
            *count -= 1;
            if *count == 0 {
                pins.remove(hash);
            }
        }
    }

    /// Serialises concurrent fetches of the same blob. Hold the returned guard while
    /// downloading, and re-check [`Store::find`] after acquiring it.
    pub async fn fetch_lock(&self, hash: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut fetching = self.fetching.lock().await;
            // Forget locks nobody holds or waits for, so the map tracks only live fetches.
            fetching.retain(|_, lock| Arc::strong_count(lock) > 1);
            fetching.entry(hash.to_owned()).or_default().clone()
        };
        lock.lock_owned().await
    }

    /// A fresh, unused path under `tmp/`.
    fn tmp_path(&self) -> PathBuf {
        let seq = self.tmp_seq.fetch_add(1, Ordering::SeqCst);
        self.tmp.join(format!("{}-{seq}", std::process::id()))
    }

    /// Starts writing a blob. `keep` stores it where the garbage collector never looks.
    pub async fn writer(&self, keep: bool) -> io::Result<BlobWriter<'_>> {
        let path = self.tmp_path();
        Ok(BlobWriter {
            store: self,
            file: tokio::fs::File::create(&path).await?,
            path: Some(path),
            hasher: blake3::Hasher::new(),
            size: 0,
            keep,
        })
    }

    /// Moves an existing file into the store as a kept blob (hashing it first).
    pub async fn adopt(&self, file: &Path) -> io::Result<(Hash, u64)> {
        let source = file.to_owned();
        let (hash, size) = tokio::task::spawn_blocking(move || hash_file(&source))
            .await
            .map_err(io::Error::other)??;
        let target = self.kept.join(&hash);
        if tokio::fs::rename(file, &target).await.is_err() {
            // Different filesystem: copy beside the store first, so a crash mid-copy never
            // leaves a truncated file under the hash name.
            let staging = self.tmp_path();
            if let Err(e) = tokio::fs::copy(file, &staging).await {
                let _ = tokio::fs::remove_file(&staging).await;
                return Err(e);
            }
            tokio::fs::rename(&staging, &target).await?;
            let _ = tokio::fs::remove_file(file).await;
        }
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok((hash, size))
    }

    /// Evicts least-recently-used cache copies when over quota or low on disk.
    /// Returns `(blobs, bytes)` removed.
    pub fn gc(&self, policy: &GcPolicy) -> io::Result<(usize, u64)> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&self.blobs)?.flatten() {
            if let (Ok(hash), Ok(meta)) = (entry.file_name().into_string(), entry.metadata())
                && is_hash(&hash)
                && let Ok(mtime) = meta.modified()
            {
                entries.push(CacheEntry {
                    hash,
                    size: meta.blocks() * 512,
                    mtime,
                });
            }
        }
        let cache_bytes = entries.iter().map(|e| e.size).sum();
        let need = bytes_to_free(cache_bytes, free_bytes(&self.blobs)?, policy);
        if need == 0 {
            return Ok((0, 0));
        }
        let pinned: HashSet<Hash> = crate::lock(&self.pins).keys().cloned().collect();
        let victims = plan_eviction(&entries, need, &pinned, policy.min_age, SystemTime::now());
        let mut freed = (0, 0);
        for victim in victims {
            if std::fs::remove_file(self.blobs.join(&victim.hash)).is_ok() {
                freed.0 += 1;
                freed.1 += victim.size;
            }
        }
        if freed.0 > 0 {
            self.version.fetch_add(1, Ordering::SeqCst);
        }
        Ok(freed)
    }
}

/// Streaming writer that only publishes the blob once its hash is known (and, when the
/// caller expected a specific hash, verified). Dropping it unfinished discards the data.
pub struct BlobWriter<'a> {
    store: &'a Store,
    file: tokio::fs::File,
    path: Option<PathBuf>,
    hasher: blake3::Hasher,
    size: u64,
    keep: bool,
}

impl BlobWriter<'_> {
    pub async fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        self.hasher.update(chunk);
        self.size += chunk.len() as u64;
        self.file.write_all(chunk).await
    }

    pub async fn finish(mut self, expected: Option<&str>) -> io::Result<(Hash, u64)> {
        self.file.flush().await?;
        self.file.sync_data().await?;
        let hash = self.hasher.finalize().to_hex().to_string();
        if let Some(expected) = expected
            && expected != hash
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("content hash mismatch: expected {expected}, received {hash}"),
            ));
        }
        let dir = if self.keep {
            &self.store.kept
        } else {
            &self.store.blobs
        };
        let tmp = self.path.take().expect("finish is called once");
        tokio::fs::rename(&tmp, dir.join(&hash)).await?;
        self.store.version.fetch_add(1, Ordering::SeqCst);
        Ok((hash, self.size))
    }
}

impl Drop for BlobWriter<'_> {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub fn hash_file(path: &Path) -> io::Result<(Hash, u64)> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let size = io::copy(&mut file, &mut hasher)?;
    Ok((hasher.finalize().to_hex().to_string(), size))
}

/// Bytes available to unprivileged writers on the filesystem holding `dir`.
pub fn free_bytes(dir: &Path) -> io::Result<u64> {
    let vfs = nix::sys::statvfs::statvfs(dir).map_err(io::Error::from)?;
    Ok(vfs.blocks_available().saturating_mul(vfs.fragment_size()))
}

/// How much cache to evict: nothing while within quota and above the free-space floor;
/// otherwise enough to get back to 80% of the quota / 125% of the floor, so the collector
/// does not run on every tick.
pub fn bytes_to_free(cache_bytes: u64, free: u64, policy: &GcPolicy) -> u64 {
    let for_quota = match policy.max_bytes {
        Some(max) if cache_bytes > max => cache_bytes - max / 10 * 8,
        _ => 0,
    };
    let for_disk = if free < policy.min_free_bytes {
        policy.min_free_bytes + policy.min_free_bytes / 4 - free
    } else {
        0
    };
    for_quota.max(for_disk)
}

/// Least recently used first, skipping pinned and recently used blobs, until `need` bytes
/// are covered.
pub fn plan_eviction<'a>(
    entries: &'a [CacheEntry],
    need: u64,
    pinned: &HashSet<Hash>,
    min_age: Duration,
    now: SystemTime,
) -> Vec<&'a CacheEntry> {
    let mut candidates: Vec<&CacheEntry> = entries
        .iter()
        .filter(|e| !pinned.contains(&e.hash))
        .filter(|e| now.duration_since(e.mtime).is_ok_and(|age| age >= min_age))
        .collect();
    candidates.sort_by(|a, b| a.mtime.cmp(&b.mtime).then_with(|| a.hash.cmp(&b.hash)));
    let mut remaining = need;
    candidates
        .into_iter()
        .take_while(|entry| {
            let take = remaining > 0;
            remaining = remaining.saturating_sub(entry.size);
            take
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1 << 30;

    fn entry(tag: char, size: u64, age_secs: u64, now: SystemTime) -> CacheEntry {
        CacheEntry {
            hash: tag.to_string().repeat(64),
            size,
            mtime: now - Duration::from_secs(age_secs),
        }
    }

    fn tags(victims: &[&CacheEntry]) -> String {
        victims.iter().map(|e| &e.hash[..1]).collect()
    }

    #[test]
    fn gc_triggers_only_over_quota_or_under_free_floor() {
        let policy = GcPolicy {
            max_bytes: Some(100 * GB),
            min_free_bytes: 10 * GB,
            min_age: Duration::ZERO,
        };
        assert_eq!(bytes_to_free(100 * GB, 50 * GB, &policy), 0);
        // Over quota: back down to 80%.
        assert_eq!(bytes_to_free(110 * GB, 50 * GB, &policy), 30 * GB);
        // Disk nearly full: restore 125% of the floor.
        assert_eq!(bytes_to_free(20 * GB, 4 * GB, &policy), 8 * GB + GB / 2);
        let unlimited = GcPolicy {
            max_bytes: None,
            ..policy
        };
        assert_eq!(bytes_to_free(900 * GB, 11 * GB, &unlimited), 0);
    }

    #[test]
    fn evicts_oldest_first_and_spares_pinned_and_recent() {
        let now = SystemTime::now();
        let entries = [
            entry('a', 10 * GB, 100, now),
            entry('b', 10 * GB, 900, now),
            entry('c', 10 * GB, 500, now),
            entry('d', 10 * GB, 800, now),
            entry('e', 10 * GB, 5, now),
        ];
        let none = HashSet::new();
        let plan = plan_eviction(&entries, 15 * GB, &none, Duration::from_secs(60), now);
        assert_eq!(tags(&plan), "bd");

        let pinned = HashSet::from(["b".repeat(64)]);
        let plan = plan_eviction(&entries, 100 * GB, &pinned, Duration::from_secs(60), now);
        assert_eq!(tags(&plan), "dca", "pinned b and just-used e survive");
        assert!(plan_eviction(&entries, 0, &none, Duration::ZERO, now).is_empty());
    }

    #[test]
    fn eviction_stops_once_enough_is_planned_and_skips_future_mtimes() {
        let now = SystemTime::now();
        let future = CacheEntry {
            hash: "f".repeat(64),
            size: GB,
            mtime: now + Duration::from_secs(3600),
        };
        let entries = [
            entry('a', 4 * GB, 300, now),
            entry('b', 4 * GB, 200, now),
            future,
        ];
        let none = HashSet::new();
        assert_eq!(
            tags(&plan_eviction(&entries, 4 * GB, &none, Duration::ZERO, now)),
            "a"
        );
        assert_eq!(
            tags(&plan_eviction(
                &entries,
                4 * GB + 1,
                &none,
                Duration::ZERO,
                now
            )),
            "ab"
        );
        // A clock skewed into the future never makes a blob look old.
        assert_eq!(
            tags(&plan_eviction(
                &entries,
                100 * GB,
                &none,
                Duration::ZERO,
                now
            )),
            "ab"
        );
    }

    #[test]
    fn floor_and_quota_together_free_the_larger_amount() {
        let policy = GcPolicy {
            max_bytes: Some(10 * GB),
            min_free_bytes: 8 * GB,
            min_age: Duration::ZERO,
        };
        // Quota wants 12 - 8 = 4 GB; disk wants 10 - 1 = 9 GB.
        assert_eq!(bytes_to_free(12 * GB, GB, &policy), 9 * GB);
    }

    #[tokio::test]
    async fn concurrent_fetch_locks_for_one_blob_are_exclusive_and_then_forgotten() {
        let root = std::env::temp_dir().join(format!("da-store-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root).unwrap();
        let first = store.fetch_lock("x").await;
        let second = tokio::time::timeout(Duration::from_millis(50), store.fetch_lock("x")).await;
        assert!(second.is_err(), "a second fetch of the same blob waits");
        drop(first);
        drop(store.fetch_lock("y").await);
        let _ = store.fetch_lock("z").await;
        assert_eq!(
            store.fetching.lock().await.len(),
            1,
            "only the newest lock is tracked"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn writes_verify_content_and_gc_respects_pins() {
        let root = std::env::temp_dir().join(format!("da-store-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root).unwrap();

        let mut w = store.writer(false).await.unwrap();
        w.write(b"hello ").await.unwrap();
        w.write(b"world").await.unwrap();
        let (hash, size) = w.finish(None).await.unwrap();
        assert_eq!(hash, blake3::hash(b"hello world").to_hex().to_string());
        assert_eq!(size, 11);
        assert_eq!(
            std::fs::read(store.find(&hash).unwrap()).unwrap(),
            b"hello world"
        );

        // A corrupted transfer is rejected and leaves nothing behind.
        let mut w = store.writer(false).await.unwrap();
        w.write(b"tampered").await.unwrap();
        assert!(w.finish(Some(&hash)).await.is_err());
        assert_eq!(std::fs::read_dir(root.join("tmp")).unwrap().count(), 0);
        assert_eq!(store.list().len(), 1);

        // Adopted outputs are kept; cache copies are evictable unless pinned.
        let out = root.join("checkpoint.bin");
        std::fs::write(&out, b"weights").unwrap();
        let (kept, _) = store.adopt(&out).await.unwrap();
        let evict_everything = GcPolicy {
            max_bytes: Some(0),
            min_free_bytes: 0,
            min_age: Duration::ZERO,
        };
        store.pin(&hash);
        assert_eq!(store.gc(&evict_everything).unwrap().0, 0);
        store.unpin(&hash);
        assert_eq!(store.gc(&evict_everything).unwrap().0, 1);
        assert!(store.find(&hash).is_none());
        assert!(store.find(&kept).is_some());
        assert!(store.find("../../etc/passwd").is_none());

        // Adopting the same content twice keeps one copy.
        std::fs::write(&out, b"weights").unwrap();
        assert_eq!(store.adopt(&out).await.unwrap().0, kept);
        assert!(!out.exists());

        std::fs::remove_dir_all(&root).unwrap();
    }
}
