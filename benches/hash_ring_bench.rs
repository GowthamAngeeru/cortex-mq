use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use cortex_mq::core::hash_ring::HashRing;

fn setup_ring(node_count: usize, vnodes: usize) -> HashRing {
    let mut ring = HashRing::new(vnodes);

    for i in 0..node_count {
        ring.add_node(&format!("node-{:03}", i), 100);
    }

    ring
}

fn bench_task_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("task_routing");

    for node_count in [3usize, 5, 10, 20, 50] {
        let ring = setup_ring(node_count, 150);

        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("get_node", node_count),
            &node_count,
            |b, _| {
                let mut counter = 0u64;
                b.iter(|| {
                    let task_id = format!("task-{:016x}", counter);
                    counter = counter.wrapping_add(1);
                    black_box(ring.get_node(black_box(&task_id)))
                });
            },
        );
    }
    group.finish();
}

fn bench_node_add_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_mutation");

    group.bench_function("add_node_to_10_node_ring", |b| {
        b.iter_with_setup(
            || setup_ring(10, 150),
            |mut ring| {
                ring.add_node(black_box("node-new"), 100);
                black_box(ring)
            },
        );
    });

    group.bench_function("remove_node_from_10_node_ring", |b| {
        b.iter_with_setup(
            || {
                let mut ring = setup_ring(10, 150);
                ring.add_node("node-target", 100);
                ring
            },
            |mut ring| {
                ring.remove_node(black_box("node-target"));
                black_box(ring)
            },
        );
    });

    group.finish();
}

fn bench_distribution_uniformity(c: &mut Criterion) {
    let mut group = c.benchmark_group("distribution");

    group.bench_function("distribution_report_10_nodes", |b| {
        let ring = setup_ring(10, 150);
        b.iter(|| black_box(ring.distribution_report()));
    });

    group.finish();
}

fn bench_failure_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("failure_routing");

    group.bench_function("route_around_dead_node_10_node_ring", |b| {
        let mut ring = setup_ring(10, 150);
        let _ = ring.mark_dead("node-000");

        let task_ids: Vec<String> = (0..100).map(|i| format!("task-{:016x}", i)).collect();

        b.iter(|| {
            for task_id in &task_ids {
                black_box(ring.get_node(black_box(task_id)));
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_task_routing,
    bench_node_add_remove,
    bench_distribution_uniformity,
    bench_failure_routing,
);
criterion_main!(benches);
