use crate::raft::state::{NodeId, Role, Term};
use rand::RngExt;
use std::time::Duration;

const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
const ELECTION_TIMEOUT_MAX_MS: u64 = 300;

#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: Role,
    pub votes_received: usize,
    pub cluster_size: usize,
}

impl RaftNode {
    pub fn new(id: &str, cluster_size: usize) -> Self {
        assert!(
            cluster_size >= 3,
            "Raft requires at least 3 nodes for meaningful fault tolerance"
        );
        assert!(
            cluster_size % 2 == 1,
            "Raft cluster size should be odd to prevent split votes"
        );

        Self {
            id: id.to_string(),
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            votes_received: 0,
            cluster_size,
        }
    }

    pub fn majority(&self) -> usize {
        (self.cluster_size / 2) + 1
    }

    pub fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes_received = 1;

        tracing::info!(
            node_id = %self.id,
            term = self.current_term,
            "raft: election_started — broadcasting RequestVote to peers"
        );
    }

    pub fn handle_vote_request(&mut self, candidate_id: &str, candidate_term: Term) -> bool {
        if candidate_term < self.current_term {
            tracing::debug!(
                node_id = %self.id,
                candidate = %candidate_id,
                candidate_term = candidate_term,
                our_term = self.current_term,
                "raft: vote_denied — stale term"
            );
            return false;
        }

        if candidate_term > self.current_term {
            tracing::info!(
                node_id = %self.id,
                old_term = self.current_term,
                new_term = candidate_term,
                "raft: stepping_down — observed higher term"
            );
            self.current_term = candidate_term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        match &self.voted_for {
            Some(existing_vote) if existing_vote != candidate_id => {
                tracing::debug!(
                    node_id = %self.id,
                    candidate_id = %candidate_id,
                    already_voted_for = %existing_vote,
                    "raft: vote_denied — already voted this term"
                );
                return false;
            }
            _ => {}
        }

        self.voted_for = Some(candidate_id.to_string());

        tracing::info!(
            node_id = %self.id,
            candidate = %candidate_id,
            term = self.current_term,
            "raft: vote_granted"
        );

        true
    }

    pub fn record_vote_response(&mut self, vote_granted: bool) {
        if self.role != Role::Candidate {
            return;
        }

        if !vote_granted {
            tracing::debug!(
                node_id = %self.id,
                "raft: vote_rejected_by_peer"
            );
            return;
        }

        self.votes_received += 1;

        tracing::debug!(
            node_id = %self.id,
            votes = self.votes_received,
            needed = self.majority(),
            "raft: vote_received"
        );

        if self.votes_received >= self.majority() {
            self.role = Role::Leader;

            tracing::info!(
                node_id = %self.id,
                term = self.current_term,
                votes = self.votes_received,
                cluster_size = self.cluster_size,
                "raft: became_leader — will begin sending heartbeats"
            );
        }
    }

    pub fn handle_heartbeat(&mut self, leader_id: &str, leader_term: Term) -> bool {
        if leader_term < self.current_term {
            tracing::warn!(
                node_id = %self.id,
                leader = %leader_id,
                leader_term = leader_term,
                our_term = self.current_term,
                "raft: heartbeat_rejected — stale leader"
            );
            return false;
        }

        if leader_term > self.current_term {
            self.current_term = leader_term;
            self.voted_for = None;
        }

        self.role = Role::Follower;

        tracing::debug!(
            node_id = %self.id,
            leader = %leader_id,
            term = self.current_term,
            "raft: heartbeat_received — election_timeout_reset"
        );

        true
    }

    pub fn random_election_timeout() -> Duration {
        let mut rng = rand::rng();
        Duration::from_millis(rng.random_range(ELECTION_TIMEOUT_MIN_MS..=ELECTION_TIMEOUT_MAX_MS))
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str) -> RaftNode {
        RaftNode::new(id, 3)
    }

    #[test]
    fn new_node_starts_as_follower() {
        let node = make_node("node-a");
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 0);
        assert!(node.voted_for.is_none());
    }

    #[test]
    fn election_increments_term_and_self_votes() {
        let mut node = make_node("node-a");
        assert_eq!(node.current_term, 0);

        node.start_election();

        assert_eq!(node.current_term, 1);
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.votes_received, 1);
        assert_eq!(node.voted_for, Some("node-a".to_string()));
    }

    #[test]
    fn majority_votes_makes_leader() {
        let mut node = make_node("node-a");
        node.start_election();
        node.record_vote_response(true);

        assert_eq!(node.role, Role::Leader);
    }

    #[test]
    fn insufficient_votes_stays_candidate() {
        let mut node = RaftNode::new("node-a", 5);
        node.start_election();
        node.record_vote_response(true);

        assert_eq!(node.role, Role::Candidate);
    }

    #[test]
    fn rejects_vote_for_stale_term() {
        let mut node = make_node("node-a");
        node.current_term = 5;

        let granted = node.handle_vote_request("node-b", 3);

        assert!(!granted);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn grants_vote_in_same_term_when_not_voted() {
        let mut node = make_node("node-a");
        node.current_term = 2;

        let granted = node.handle_vote_request("node-b", 2);

        assert!(granted);
        assert_eq!(node.voted_for, Some("node-b".to_string()));
    }

    #[test]
    fn one_vote_per_term_enforced() {
        let mut node = make_node("node-a");

        let first = node.handle_vote_request("node-b", 1);
        let second = node.handle_vote_request("node-c", 1);

        assert!(first);
        assert!(!second);
    }

    #[test]
    fn steps_down_and_votes_on_higher_term() {
        let mut node = make_node("node-a");
        node.current_term = 2;
        node.role = Role::Candidate;

        let granted = node.handle_vote_request("node-b", 5);

        assert!(granted);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn valid_heartbeat_resets_to_follower() {
        let mut node = make_node("node-a");
        node.role = Role::Candidate;
        node.current_term = 2;

        let accepted = node.handle_heartbeat("node-b", 2);

        assert!(accepted);
        assert_eq!(node.role, Role::Follower);
    }

    #[test]
    fn stale_heartbeat_rejected() {
        let mut node = make_node("node-a");
        node.current_term = 5;

        let accepted = node.handle_heartbeat("node-b", 3);

        assert!(!accepted);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn majority_calculation_correct() {
        assert_eq!(RaftNode::new("x", 3).majority(), 2);
        assert_eq!(RaftNode::new("x", 5).majority(), 3);
        assert_eq!(RaftNode::new("x", 7).majority(), 4);
    }

    #[test]
    fn ignores_vote_response_when_no_longer_candidate() {
        let mut node = make_node("node-a");
        node.start_election();
        node.role = Role::Follower;

        node.record_vote_response(true);

        assert_eq!(node.role, Role::Follower);
    }
}
