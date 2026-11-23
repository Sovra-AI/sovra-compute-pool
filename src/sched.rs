//! Placement: decides which node (and which GPUs) each replica of a job runs on.
//!
//! Pure functions over a snapshot of free capacity, so the policy is testable in isolation:
//! * gang scheduling — every replica is placed or none are;
//! * data locality — prefer the node that already caches the most input bytes;
//! * GPU best-fit — use the smallest adequate GPUs, and keep GPU nodes free of CPU-only work.

use std::collections::{BTreeMap, HashMap};

use crate::proto::{Hash, JobSpec};

/// Unallocated capacity of one ready node.
#[derive(Clone, Debug)]
pub struct NodeFree<'a> {
    pub name: &'a str,
    pub cpus: u32,
    pub memory_mb: u64,
    /// `(index, vram_mb)` of GPUs not allocated to any task.
    pub gpus: Vec<(u32, u64)>,
    pub labels: &'a BTreeMap<String, String>,
    /// Blobs cached on the node, if it has reported its inventory.
    pub blobs: Option<&'a HashMap<Hash, u64>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Placement {
    pub node: String,
    pub gpus: Vec<u32>,
}

struct Candidate<'a> {
    index: usize,
    free: NodeFree<'a>,
    /// Cards before this index are too small. We only ever remove cards after it.
    first_gpu: usize,
    shared_bytes: u64,
}

impl<'a> Candidate<'a> {
    fn new(index: usize, node: &NodeFree<'a>, spec: &JobSpec) -> Option<Self> {
        let need = &spec.resources;
        let gpu_count = need.gpus as usize;
        if node.cpus < need.cpus || node.memory_mb < need.memory_mb || node.gpus.len() < gpu_count {
            return None;
        }
        let mut free = node.clone();
        let first_gpu = if gpu_count == 0 {
            0
        } else {
            free.gpus
                .sort_unstable_by_key(|&(index, vram)| (vram, index));
            free.gpus
                .partition_point(|&(_, vram)| vram < need.gpu_memory_mb)
        };
        if free.gpus.len() - first_gpu < gpu_count {
            return None;
        }
        let shared_bytes = free
            .blobs
            .map_or(0, |have| cached_bytes(spec.inputs.iter(), have));
        Some(Self {
            index,
            free,
            first_gpu,
            shared_bytes,
        })
    }
}

/// Places every replica of `spec`, deducting from `nodes` on success. On failure `nodes` is
/// left untouched and the error says why the job is still waiting.
pub fn place(spec: &JobSpec, nodes: &mut [NodeFree<'_>]) -> Result<Vec<Placement>, String> {
    let need = &spec.resources;
    let matches_selector = |node: &NodeFree<'_>| {
        spec.selector
            .iter()
            .all(|(key, value)| node.labels.get(key) == Some(value))
    };
    if nodes.is_empty() {
        return Err("no ready nodes".into());
    }
    // Score shared inputs at most once per candidate. Only the round-robin shards
    // differ by rank. Capacity is speculative until every rank has a placement.
    let gpu_count = need.gpus as usize;
    let mut matched = false;
    let mut candidates = Vec::new();
    for (index, node) in nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| matches_selector(node))
    {
        matched = true;
        if let Some(candidate) = Candidate::new(index, node, spec) {
            candidates.push(candidate);
        }
    }
    if !matched {
        return Err("no ready node matches the selector".into());
    }

    let mut placements = Vec::new();
    for rank in 0..spec.replicas {
        let best = candidates
            .iter_mut()
            .filter(|c| {
                c.free.cpus >= need.cpus
                    && c.free.memory_mb >= need.memory_mb
                    && c.free.gpus.len() - c.first_gpu >= gpu_count
            })
            .min_by_key(|c| {
                let node = &c.free;
                let shards = node
                    .blobs
                    .map_or(0, |have| cached_bytes(spec.shards_for_rank(rank), have));
                let cached = c.shared_bytes.saturating_add(shards);
                // Preserve larger cards when otherwise equally suitable nodes compete.
                let largest_card = if gpu_count == 0 {
                    0
                } else {
                    node.gpus[c.first_gpu + gpu_count - 1].1
                };
                (
                    std::cmp::Reverse(cached),
                    node.gpus.len() - gpu_count,
                    largest_card,
                    std::cmp::Reverse(node.cpus),
                    node.name,
                )
            });
        let Some(candidate) = best else {
            return Err(format!(
                "waiting for capacity: {rank} of {} replicas fit right now (each needs {} cpu, {} MB{})",
                spec.replicas,
                need.cpus,
                need.memory_mb,
                match (need.gpus, need.gpu_memory_mb) {
                    (0, _) => String::new(),
                    (n, 0) => format!(", {n} gpu"),
                    (n, vram) => format!(", {n} gpu with >= {vram} MB"),
                }
            ));
        };

        let node = &mut candidate.free;
        node.cpus -= need.cpus;
        node.memory_mb -= need.memory_mb;
        let mut taken: Vec<u32> = node
            .gpus
            .drain(candidate.first_gpu..candidate.first_gpu + gpu_count)
            .map(|(index, _)| index)
            .collect();
        taken.sort_unstable();
        placements.push(Placement {
            node: node.name.to_owned(),
            gpus: taken,
        });
    }
    for candidate in candidates {
        nodes[candidate.index] = candidate.free;
    }
    Ok(placements)
}

fn cached_bytes<'a>(
    inputs: impl Iterator<Item = &'a crate::proto::Blob>,
    have: &HashMap<Hash, u64>,
) -> u64 {
    inputs
        .filter(|blob| have.contains_key(&blob.hash))
        .fold(0, |bytes, blob| bytes.saturating_add(blob.size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Blob, Resources};

    fn spec(cpus: u32, gpus: u32, replicas: u32) -> JobSpec {
        JobSpec {
            name: String::new(),
            command: vec!["true".into()],
            env: BTreeMap::new(),
            resources: Resources {
                cpus,
                memory_mb: 1024,
                gpus,
                gpu_memory_mb: 0,
            },
            replicas,
            inputs: Vec::new(),
            shards: Vec::new(),
            selector: BTreeMap::new(),
            max_retries: 0,
            priority: 0,
        }
    }

    fn node<'a>(
        name: &'a str,
        cpus: u32,
        gpus: &[u64],
        labels: &'a BTreeMap<String, String>,
    ) -> NodeFree<'a> {
        NodeFree {
            name,
            cpus,
            memory_mb: 64_000,
            gpus: gpus.iter().zip(0..).map(|(v, i)| (i, *v)).collect(),
            labels,
            blobs: None,
        }
    }

    #[test]
    fn gang_is_all_or_nothing() {
        let labels = BTreeMap::new();
        let mut nodes = vec![
            node("a", 8, &[24_000], &labels),
            node("b", 8, &[24_000], &labels),
        ];

        let err = place(&spec(1, 1, 3), &mut nodes).unwrap_err();
        assert!(err.contains("2 of 3"), "{err}");
        assert_eq!(
            nodes[0].gpus.len() + nodes[1].gpus.len(),
            2,
            "nothing was reserved"
        );

        let placed = place(&spec(1, 1, 2), &mut nodes).unwrap();
        let mut hosts: Vec<&str> = placed.iter().map(|p| p.node.as_str()).collect();
        hosts.sort_unstable();
        assert_eq!(hosts, ["a", "b"]);
        assert!(nodes.iter().all(|n| n.gpus.is_empty()));
        assert!(place(&spec(1, 1, 1), &mut nodes).is_err());
    }

    #[test]
    fn cpu_jobs_avoid_gpu_nodes_and_gpu_jobs_take_the_smallest_adequate_card() {
        let labels = BTreeMap::new();
        let mut nodes = vec![
            node("gpu", 32, &[12_000, 48_000], &labels),
            node("cpu", 4, &[], &labels),
        ];

        let placed = place(&spec(2, 0, 1), &mut nodes).unwrap();
        assert_eq!(placed[0].node, "cpu");

        let placed = place(&spec(2, 1, 1), &mut nodes).unwrap();
        assert_eq!(
            placed[0],
            Placement {
                node: "gpu".into(),
                gpus: vec![0]
            }
        );

        let mut big = spec(2, 1, 1);
        big.resources.gpu_memory_mb = 40_000;
        assert_eq!(place(&big, &mut nodes).unwrap()[0].gpus, [1]);
    }

    #[test]
    fn equally_local_nodes_preserve_the_larger_card_for_the_next_job() {
        let labels = BTreeMap::new();
        let mut nodes = vec![
            node("large", 32, &[48_000], &labels),
            node("small", 4, &[12_000], &labels),
        ];
        assert_eq!(place(&spec(1, 1, 1), &mut nodes).unwrap()[0].node, "small");
        let mut big = spec(1, 1, 1);
        big.resources.gpu_memory_mb = 40_000;
        assert_eq!(place(&big, &mut nodes).unwrap()[0].node, "large");
    }

    #[test]
    fn rank_local_shards_are_scored_separately_from_shared_inputs() {
        let labels = BTreeMap::new();
        let blob = |name: &str, size| Blob {
            name: name.into(),
            hash: name.into(),
            size,
        };
        let mut job = spec(1, 0, 2);
        job.inputs = vec![blob("common", 20)];
        job.shards = vec![blob("left", 100), blob("right", 100)];
        let a = HashMap::from([("common".into(), 20), ("right".into(), 100)]);
        let b = HashMap::from([("left".into(), 100)]);
        let mut nodes = vec![node("a", 8, &[], &labels), node("b", 8, &[], &labels)];
        nodes[0].blobs = Some(&a);
        nodes[1].blobs = Some(&b);
        let placed = place(&job, &mut nodes).unwrap();
        assert_eq!(placed[0].node, "b");
        assert_eq!(placed[1].node, "a");
    }

    #[test]
    fn prefers_the_node_that_already_has_the_data_and_honours_selectors() {
        let shard = Blob {
            name: "shard-0".into(),
            hash: "a".repeat(64),
            size: 128 << 20,
        };
        let warm_cache = HashMap::from([(shard.hash.clone(), shard.size)]);
        let plain = BTreeMap::new();
        let office = BTreeMap::from([("site".to_owned(), "office".to_owned())]);
        let mut nodes = vec![node("cold", 64, &[], &plain), node("warm", 4, &[], &office)];
        nodes[1].blobs = Some(&warm_cache);

        let mut job = spec(1, 0, 1);
        job.inputs = vec![shard];
        assert_eq!(place(&job, &mut nodes).unwrap()[0].node, "warm");

        let mut pinned = spec(1, 0, 1);
        pinned.selector = BTreeMap::from([("site".to_owned(), "lab".to_owned())]);
        assert_eq!(
            place(&pinned, &mut nodes).unwrap_err(),
            "no ready node matches the selector"
        );
    }
}
