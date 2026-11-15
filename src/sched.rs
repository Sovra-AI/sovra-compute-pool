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

/// Places every replica of `spec`, deducting from `nodes` on success. On failure `nodes` is
/// left untouched and the error says why the job is still waiting.
pub fn place(spec: &JobSpec, nodes: &mut Vec<NodeFree<'_>>) -> Result<Vec<Placement>, String> {
    let need = &spec.resources;
    let matches_selector = |node: &NodeFree<'_>| {
        spec.selector
            .iter()
            .all(|(key, value)| node.labels.get(key) == Some(value))
    };
    if nodes.is_empty() {
        return Err("no ready nodes".into());
    }
    if !nodes.iter().any(matches_selector) {
        return Err("no ready node matches the selector".into());
    }

    let mut trial = nodes.clone();
    let mut placements = Vec::new();
    for rank in 0..spec.replicas {
        let inputs = spec.inputs_for_rank(rank);
        let best = trial
            .iter()
            .enumerate()
            .filter(|(_, node)| matches_selector(node))
            .filter(|(_, node)| node.cpus >= need.cpus && node.memory_mb >= need.memory_mb)
            .filter_map(|(i, node)| {
                let usable = node
                    .gpus
                    .iter()
                    .filter(|(_, vram)| *vram >= need.gpu_memory_mb)
                    .count();
                if usable < need.gpus as usize {
                    return None;
                }
                let cached: u64 = inputs
                    .iter()
                    .filter(|b| node.blobs.is_some_and(|have| have.contains_key(&b.hash)))
                    .map(|b| b.size)
                    .sum();
                let gpus_left = node.gpus.len() - need.gpus as usize;
                // Most cached input bytes, then fewest GPUs left idle, then most free CPU.
                Some((i, (std::cmp::Reverse(cached), gpus_left, std::cmp::Reverse(node.cpus), node.name)))
            })
            .min_by(|a, b| a.1.cmp(&b.1))
            .map(|(i, _)| i);

        let Some(i) = best else {
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

        let node = &mut trial[i];
        node.cpus -= need.cpus;
        node.memory_mb -= need.memory_mb;
        // Smallest adequate GPUs first, so large-VRAM cards stay available.
        let mut adequate: Vec<(u32, u64)> = node
            .gpus
            .iter()
            .copied()
            .filter(|(_, vram)| *vram >= need.gpu_memory_mb)
            .collect();
        adequate.sort_by_key(|&(index, vram)| (vram, index));
        let mut taken: Vec<u32> = adequate
            .iter()
            .take(need.gpus as usize)
            .map(|&(index, _)| index)
            .collect();
        taken.sort_unstable();
        node.gpus.retain(|(index, _)| !taken.contains(index));
        placements.push(Placement {
            node: node.name.to_owned(),
            gpus: taken,
        });
    }
    *nodes = trial;
    Ok(placements)
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
            gpus: gpus.iter().enumerate().map(|(i, v)| (i as u32, *v)).collect(),
            labels,
            blobs: None,
        }
    }

    #[test]
    fn gang_is_all_or_nothing() {
        let labels = BTreeMap::new();
        let mut nodes = vec![node("a", 8, &[24_000], &labels), node("b", 8, &[24_000], &labels)];

        let err = place(&spec(1, 1, 3), &mut nodes).unwrap_err();
        assert!(err.contains("2 of 3"), "{err}");
        assert_eq!(nodes[0].gpus.len() + nodes[1].gpus.len(), 2, "nothing was reserved");

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
        let mut nodes = vec![node("gpu", 32, &[12_000, 48_000], &labels), node("cpu", 4, &[], &labels)];

        let placed = place(&spec(2, 0, 1), &mut nodes).unwrap();
        assert_eq!(placed[0].node, "cpu");

        let placed = place(&spec(2, 1, 1), &mut nodes).unwrap();
        assert_eq!(placed[0], Placement { node: "gpu".into(), gpus: vec![0] });

        let mut big = spec(2, 1, 1);
        big.resources.gpu_memory_mb = 40_000;
        assert_eq!(place(&big, &mut nodes).unwrap()[0].gpus, [1]);
    }

    #[test]
    fn prefers_the_node_that_already_has_the_data_and_honours_selectors() {
        let shard = Blob { name: "shard-0".into(), hash: "a".repeat(64), size: 128 << 20 };
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
        assert_eq!(place(&pinned, &mut nodes).unwrap_err(), "no ready node matches the selector");
    }
}
