use std::collections::{BTreeMap, HashMap, HashSet};

const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

const CPU_SOFT_LIMIT: f32 = 90.0;
const CPU_HARD_LIMIT: f32 = 100.0;
const MISSED_HEARTBEAT_LIMIT: u32 = 3;

#[inline]
fn fnv1a(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;

    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    hash
}

#[inline]
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Suspect,
    Dead,
    Draining,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Healthy => write!(f, "healthy"),
            NodeStatus::Suspect => write!(f, "suspect"),
            NodeStatus::Dead => write!(f, "dead"),
            NodeStatus::Draining => write!(f, "draining"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub active_tasks: u32,
    pub max_concurrent: u32,
    pub cpu_percent: f32,
    pub status: NodeStatus,
    pub last_heartbeat_unix: i64,
    pub missed_heartbeats: u32,
    pub total_completed: u64,
}

impl NodeInfo {
    pub fn new(node_id: &str, max_concurrent: u32) -> Self {
        Self {
            node_id: node_id.to_string(),
            active_tasks: 0,
            max_concurrent,
            cpu_percent: 0.0,
            status: NodeStatus::Healthy,
            last_heartbeat_unix: now_unix(),
            missed_heartbeats: 0,
            total_completed: 0,
        }
    }

    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.status != NodeStatus::Dead
    }

    #[must_use]
    pub fn is_schedulable(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy)
    }

    #[must_use]
    pub fn has_capacity(&self) -> bool {
        self.is_schedulable() && self.active_tasks < self.max_concurrent
    }

    #[must_use]
    pub fn load_factor(&self) -> f32 {
        if self.max_concurrent == 0 {
            return 1.0;
        }

        self.active_tasks as f32 / self.max_concurrent as f32
    }
}

pub struct HashRing {
    ring: BTreeMap<u64, String>,
    nodes: HashMap<String, NodeInfo>,
    virtual_nodes: usize,
}

impl HashRing {
    #[must_use]
    pub fn new(virtual_nodes: usize) -> Self {
        assert!(virtual_nodes > 0, "virtual_nodes must be > 0");
        assert!(virtual_nodes <= 1000, "virtual_nodes too large");

        Self {
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
            virtual_nodes,
        }
    }

    pub fn add_node(&mut self, node_id: &str, max_concurrent: u32) {
        let capacity = max_concurrent.max(1);

        let is_rejoin = self.nodes.contains_key(node_id);
        if is_rejoin {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.status = NodeStatus::Healthy;
                node.max_concurrent = capacity;
                node.last_heartbeat_unix = now_unix();
                node.missed_heartbeats = 0;
            }
            return;
        }

        let owned = node_id.to_string();
        let mut collisions = 0usize;

        'vnode_loop: for i in 0..self.virtual_nodes {
            let mut key = format!("{}:vnode:{}", node_id, i);
            let mut pos = fnv1a(key.as_bytes());
            let mut attempts = 0usize;

            while self.ring.contains_key(&pos) {
                collisions += 1;
                attempts += 1;

                if attempts > 3 {
                    tracing::warn!(
                        node_id = %node_id,
                        vnode = i,
                        "hash_ring: vnode skipped after collisions"
                    );
                    continue 'vnode_loop;
                }

                key = format!("{}:vnode:{}:salt:{}", node_id, i, attempts);
                pos = fnv1a(key.as_bytes());
            }

            self.ring.insert(pos, owned.clone());
        }

        self.nodes
            .insert(owned.clone(), NodeInfo::new(&owned, capacity));

        tracing::info!(
            node_id = %node_id,
            rejoin = is_rejoin,
            collisions = collisions,
            ring_size = self.ring.len(),
            "hash_ring: node_added"
        );
    }

    pub fn remove_node(&mut self, node_id: &str) {
        let keys: Vec<u64> = self
            .ring
            .iter()
            .filter(|(_, owner)| owner.as_str() == node_id)
            .map(|(k, _)| *k)
            .collect();

        for key in keys {
            self.ring.remove(&key);
        }

        let existed = self.nodes.remove(node_id).is_some();

        if !existed {
            tracing::warn!(
                node_id = %node_id,
                "hash_ring: remove_node unknown node"
            );
        }

        tracing::warn!(
            node_id = %node_id,
            ring_size = self.ring.len(),
            active_nodes = self.nodes.len(),
            "hash_ring: node_removed"
        );
    }

    pub fn update_node_info(&mut self, node_id: &str, active_tasks: u32, cpu_percent: f32) {
        match self.nodes.get_mut(node_id) {
            None => {
                tracing::warn!(
                    node_id = %node_id,
                    "hash_ring: heartbeat_from_unknown_node"
                );
            }

            Some(info) => {
                let cpu = cpu_percent.clamp(0.0, CPU_HARD_LIMIT);
                let revived = info.status == NodeStatus::Dead;

                info.active_tasks = active_tasks;
                info.cpu_percent = cpu;
                info.last_heartbeat_unix = now_unix();
                info.missed_heartbeats = 0;

                info.status = if cpu >= CPU_SOFT_LIMIT {
                    NodeStatus::Suspect
                } else {
                    NodeStatus::Healthy
                };

                if revived {
                    tracing::info!(
                        node_id = %node_id,
                        "hash_ring: node_revived"
                    );
                }

                if active_tasks > info.max_concurrent {
                    tracing::warn!(
                        node_id = %node_id,
                        active_tasks = active_tasks,
                        max_tasks = info.max_concurrent,
                        "hash_ring: node_over_capacity"
                    );
                }
            }
        }
    }

    pub fn record_missed_heartbeat(&mut self, node_id: &str) -> Option<u32> {
        match self.nodes.get_mut(node_id) {
            None => {
                tracing::warn!(
                    node_id = %node_id,
                    "hash_ring: missed_heartbeat_unknown_node"
                );
                None
            }

            Some(info) => {
                info.missed_heartbeats = info.missed_heartbeats.saturating_add(1);

                info.status = if info.missed_heartbeats >= MISSED_HEARTBEAT_LIMIT {
                    NodeStatus::Dead
                } else {
                    NodeStatus::Suspect
                };

                tracing::warn!(
                    node_id = %node_id,
                    missed = info.missed_heartbeats,
                    status = ?info.status,
                    "hash_ring: missed_heartbeat"
                );

                Some(info.missed_heartbeats)
            }
        }
    }

    #[must_use]
    pub fn mark_dead(&mut self, node_id: &str) -> bool {
        match self.nodes.get_mut(node_id) {
            Some(info) => {
                info.status = NodeStatus::Dead;
                info.cpu_percent = 0.0;

                tracing::warn!(
                    node_id = %node_id,
                    orphaned_tasks = info.active_tasks,
                    "hash_ring: node_marked_dead"
                );

                true
            }

            None => {
                tracing::warn!(
                    node_id = %node_id,
                    "hash_ring: mark_dead_unknown_node"
                );
                false
            }
        }
    }

    #[must_use]
    pub fn mark_draining(&mut self, node_id: &str) -> bool {
        match self.nodes.get_mut(node_id) {
            Some(info) => {
                info.status = NodeStatus::Draining;

                tracing::info!(
                    node_id = %node_id,
                    "hash_ring: node_marked_draining"
                );

                true
            }

            None => {
                tracing::warn!(
                    node_id = %node_id,
                    "hash_ring: mark_draining_unknown_node"
                );
                false
            }
        }
    }

    #[must_use]
    pub fn get_node(&self, task_id: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = fnv1a(task_id.as_bytes());
        let mut rejected: HashSet<&str> = HashSet::new();

        for (_, node_id) in self.ring.range(hash..).chain(self.ring.iter()) {
            let node = node_id.as_str();

            if rejected.contains(node) {
                continue;
            }

            match self.nodes.get(node) {
                None => {
                    rejected.insert(node);
                }

                Some(info) if !info.has_capacity() || info.cpu_percent >= CPU_SOFT_LIMIT => {
                    rejected.insert(node);
                }

                Some(_) => return Some(node),
            }
        }

        None
    }

    #[must_use]
    pub fn get_node_force(&self, task_id: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = fnv1a(task_id.as_bytes());
        let mut rejected: HashSet<&str> = HashSet::new();

        for (_, node_id) in self.ring.range(hash..).chain(self.ring.iter()) {
            let node = node_id.as_str();

            if rejected.contains(node) {
                continue;
            }

            match self.nodes.get(node) {
                Some(info) if info.is_alive() => return Some(node),
                _ => {
                    rejected.insert(node);
                }
            }
        }

        None
    }

    pub fn record_task_completed(&mut self, node_id: &str) {
        if let Some(info) = self.nodes.get_mut(node_id) {
            info.total_completed = info.total_completed.saturating_add(1);

            info.active_tasks = info.active_tasks.saturating_sub(1);
        }
    }

    #[must_use]
    pub fn active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes.values().filter(|node| node.is_alive()).collect()
    }

    #[must_use]
    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }

    #[must_use]
    pub fn distribution_report(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();

        for node_id in self.ring.values() {
            *counts.entry(node_id.clone()).or_insert(0) += 1;
        }

        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ring() -> HashRing {
        let mut ring = HashRing::new(100);
        ring.add_node("node-a", 5);
        ring.add_node("node-b", 5);
        ring.add_node("node-c", 5);
        ring
    }

    #[test]
    fn add_remove_node() {
        let mut ring = make_ring();
        assert_eq!(ring.active_nodes().len(), 3);

        ring.remove_node("node-a");
        assert_eq!(ring.active_nodes().len(), 2);
    }

    #[test]
    fn skips_dead_node() {
        let mut ring = make_ring();

        let first = ring.get_node("task-1").unwrap().to_string();
        let _ = ring.mark_dead(&first);

        let second = ring.get_node("task-1").unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn skips_full_node() {
        let mut ring = make_ring();

        ring.update_node_info("node-a", 5, 10.0);

        for i in 0..50 {
            let task = format!("task-{}", i);
            let node = ring.get_node(&task).unwrap();
            assert_ne!(node, "node-a");
        }
    }

    #[test]
    fn skips_cpu_hot_node() {
        let mut ring = make_ring();

        ring.update_node_info("node-a", 1, 95.0);

        for i in 0..50 {
            let task = format!("task-{}", i);
            let node = ring.get_node(&task).unwrap();
            assert_ne!(node, "node-a");
        }
    }

    #[test]
    fn force_route_when_all_busy() {
        let mut ring = make_ring();

        ring.update_node_info("node-a", 5, 95.0);
        ring.update_node_info("node-b", 5, 95.0);
        ring.update_node_info("node-c", 5, 95.0);

        assert!(ring.get_node("x").is_none());
        assert!(ring.get_node_force("x").is_some());
    }

    #[test]
    fn fair_distribution() {
        let ring = make_ring();
        let report = ring.distribution_report();

        for (_, count) in report {
            assert!(count > 90 && count < 110);
        }
    }
}
