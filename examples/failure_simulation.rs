//! Demonstrates hash ring behavior under node failure and recovery.
//! No broker required — runs entirely in memory.
//! Run with: cargo run --example failure_simulation

use cortex_mq::core::hash_ring::HashRing;
use std::cmp::Reverse;

fn main() {
    println!("=== Cortex-MQ Hash Ring Failure Simulation ===\n");

    // Build a 5-node ring
    let mut ring = HashRing::new(150);
    for name in &["alpha", "beta", "gamma", "delta", "epsilon"] {
        ring.add_node(name, 10);
        println!("Added node: {}", name);
    }
    println!("Ring size: {} virtual nodes\n", ring.ring_size());

    // Route 10 tasks — collect owned Strings, not &str references
    // This is required: get_node returns &str tied to ring's lifetime.
    // Holding those references would prevent the later mutable borrow.
    println!("--- Initial routing (all nodes healthy) ---");
    let tasks: Vec<String> = (0..10).map(|i| format!("task-{:04}", i)).collect();

    let initial_routes: Vec<(String, String)> = tasks
        .iter()
        .map(|t| {
            let node = ring.get_node(t).unwrap().to_string(); // .to_string() breaks the borrow
            println!("  {} → {}", t, node);
            (t.clone(), node)
        })
        .collect();

    // Simulate crash of the node that owns the first task
    let victim = initial_routes[0].1.clone();
    println!("\n--- Simulating crash of node '{}' ---", victim);
    let _ = ring.mark_dead(&victim); // ring is no longer borrowed — safe
    println!("Node '{}' marked Dead\n", victim);

    // Re-route the same tasks and count how many moved
    println!("--- Routing after failure ---");
    let mut rerouted = 0usize;

    for (task, original_node) in &initial_routes {
        let new_node = ring.get_node(task).unwrap_or("[no healthy node]");
        let moved = new_node != original_node.as_str();
        if moved {
            rerouted += 1;
        }
        println!(
            "  {} → {}{}",
            task,
            new_node,
            if moved { "  ← rerouted" } else { "" }
        );
    }

    println!(
        "\n{}/{} tasks rerouted after '{}' crashed",
        rerouted,
        tasks.len(),
        victim
    );
    println!(
        "Consistent hashing: only tasks that hashed near '{}' moved.",
        victim
    );

    // Node revival
    println!("\n--- Node '{}' comes back online ---", victim);
    ring.add_node(&victim, 10);
    println!(
        "Node '{}' rejoined — zero downtime, no manual intervention\n",
        victim
    );

    // Distribution report
    println!("--- Virtual node distribution after rejoin ---");
    let mut report: Vec<(String, usize)> = ring.distribution_report().into_iter().collect();
    report.sort_by_key(|(_, count)| Reverse(*count));

    let total: usize = report.iter().map(|(_, c)| c).sum();
    for (node, count) in &report {
        let pct = (*count as f64 / total as f64) * 100.0;
        println!("  {:<12} {:>4} vnodes ({:.1}%)", node, count, pct);
    }

    println!("\nSimulation complete.");
    println!("The broker routes around failures automatically.");
    println!("Only tasks hashed near the dead node's virtual positions are reassigned.");
}
