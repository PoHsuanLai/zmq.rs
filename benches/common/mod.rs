//! Shared bench helpers. Each bench file pulls these in via
//! `#[path = "common/mod.rs"] mod common;` (cargo bench harnesses are
//! independent crate roots, so there's no shared lib path).

#![allow(dead_code)] // not every bench file uses every helper

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::{Builder, Runtime};

pub const MSG_SIZES: &[usize] = &[16, 256, 4096, 65536];

/// Fanout sub counts: 1 baseline, 8 real fanout. Higher counts blow
/// criterion's adaptive warmup budget when many `SubSocket`s contend
/// for the 2-worker tokio runtime.
pub const SUB_COUNTS: &[usize] = &[1, 8];

/// Per-sub arrival times for fanout benches. Criterion's headline is
/// barrier time (slowest straggler dominates) — we also record per-sub
/// p50/p99 to a sibling `arrival.json` for typical-case latency.
#[derive(Default)]
pub struct ArrivalStats {
    samples_ns: parking_lot::Mutex<Vec<u64>>,
    barrier_ns: parking_lot::Mutex<Vec<u64>>,
}

impl ArrivalStats {
    pub fn record_per_sub(&self, ns: u64) {
        self.samples_ns.lock().push(ns);
    }
    pub fn record_barrier(&self, ns: u64) {
        self.barrier_ns.lock().push(ns);
    }
    pub fn write(&self, bench_group: &str, bench_input: &str) {
        use std::io::Write;
        let mut per_sub = self.samples_ns.lock().clone();
        let mut barrier = self.barrier_ns.lock().clone();
        per_sub.sort_unstable();
        barrier.sort_unstable();
        let p = |v: &[u64], q: f64| -> u64 {
            if v.is_empty() {
                return 0;
            }
            let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
            v[idx.min(v.len() - 1)]
        };
        let mean = |v: &[u64]| -> u64 {
            if v.is_empty() {
                0
            } else {
                v.iter().sum::<u64>() / v.len() as u64
            }
        };
        let group_dir = bench_group.replace('/', "_");
        let dir = std::path::PathBuf::from("target/criterion")
            .join(&group_dir)
            .join(bench_input);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("arrival-stats: mkdir {:?} failed: {e}", dir);
            return;
        }
        let path = dir.join("arrival.json");
        let json = format!(
            r#"{{
  "per_sub_ns": {{
    "count": {count_ps},
    "mean": {mean_ps},
    "p50": {p50_ps},
    "p95": {p95_ps},
    "p99": {p99_ps},
    "max": {max_ps}
  }},
  "barrier_ns": {{
    "count": {count_br},
    "mean": {mean_br},
    "p50": {p50_br},
    "p95": {p95_br},
    "p99": {p99_br},
    "max": {max_br}
  }}
}}
"#,
            count_ps = per_sub.len(),
            mean_ps = mean(&per_sub),
            p50_ps = p(&per_sub, 0.50),
            p95_ps = p(&per_sub, 0.95),
            p99_ps = p(&per_sub, 0.99),
            max_ps = per_sub.last().copied().unwrap_or(0),
            count_br = barrier.len(),
            mean_br = mean(&barrier),
            p50_br = p(&barrier, 0.50),
            p95_br = p(&barrier, 0.95),
            p99_br = p(&barrier, 0.99),
            max_br = barrier.last().copied().unwrap_or(0),
        );
        match std::fs::File::create(&path) {
            Ok(mut f) => {
                if let Err(e) = f.write_all(json.as_bytes()) {
                    eprintln!("arrival-stats: write {:?} failed: {e}", path);
                }
            }
            Err(e) => eprintln!("arrival-stats: create {:?} failed: {e}", path),
        }
    }
}

/// Monotonic suffix so ipc paths don't collide across bench cases.
static IPC_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn ipc_path(tag: &str) -> String {
    let n = IPC_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("ipc:///tmp/libzmq-bench-{}-{}-{}.sock", tag, pid, n)
}

static ZMQRS_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn zmqrs_ipc_path(tag: &str) -> String {
    let n = ZMQRS_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("ipc:///tmp/zmqrs-cmp-{}-{}-{}.sock", tag, pid, n)
}

/// Pinned at 2 workers (driver + peer) so RTT numbers don't move with
/// host CPU count. Tokio's default scales to ncpu, which churns scheduler
/// behavior between machines.
pub fn build_rt() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime")
}
