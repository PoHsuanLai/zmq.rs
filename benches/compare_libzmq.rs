//! Side-by-side comparison against libzmq (via zmq2 bindings).
//!
//! Per-pairing modules under `compare_libzmq/` own libzmq + zmq.rs benches
//! for one socket-type pair. Shared helpers live in `common/` next to this
//! file so other bench binaries can reuse them.
//!
//! libzmq peers run on dedicated OS threads (libzmq sockets aren't
//! thread-safe). zmq.rs peers run on a shared 2-worker tokio runtime
//! (fixed for reproducibility across hosts). Transports: tcp, ipc.

use criterion::{criterion_group, criterion_main};

#[path = "common/mod.rs"]
mod common;

#[path = "compare_libzmq/pub_sub.rs"]
mod pub_sub;
#[path = "compare_libzmq/pub_xsub.rs"]
mod pub_xsub;
#[path = "compare_libzmq/push_pull.rs"]
mod push_pull;
#[path = "compare_libzmq/req_rep.rs"]
mod req_rep;
#[path = "compare_libzmq/xpub_sub.rs"]
mod xpub_sub;

criterion_group!(
    benches,
    pub_sub::bench_pub_sub,
    pub_sub::bench_zmqrs_pub_sub,
    req_rep::bench_req_rep,
    req_rep::bench_zmqrs_req_rep,
    push_pull::bench_push_pull,
    push_pull::bench_zmqrs_push_pull,
    pub_xsub::bench_pub_xsub,
    pub_xsub::bench_zmqrs_pub_xsub,
    xpub_sub::bench_xpub_sub,
    xpub_sub::bench_zmqrs_xpub_sub,
);
criterion_main!(benches);
