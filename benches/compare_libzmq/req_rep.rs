//! REQ ↔ REP round trip — symmetric latency.

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{prelude::*, RepSocket, ReqSocket, ZmqMessage};

use crate::common::{build_rt, ipc_path, zmqrs_ipc_path, MSG_SIZES};

// ── libzmq side ──────────────────────────────────────────────────────────────

pub fn bench_req_rep(c: &mut Criterion) {
    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("libzmq/req_rep/{}", transport));
        group.sample_size(30);
        group.measurement_time(Duration::from_secs(10));
        group.warm_up_time(Duration::from_secs(2));

        for &msg_size in MSG_SIZES {
            group.throughput(Throughput::Bytes(msg_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(msg_size),
                &msg_size,
                |b, &msg_size| {
                    let endpoint = match transport {
                        "tcp" => "tcp://127.0.0.1:0".to_string(),
                        "ipc" => ipc_path(&format!("reqrep-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_libzmq_req_rep(b, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_libzmq_req_rep(b: &mut criterion::Bencher<'_>, msg_size: usize, endpoint: &str) {
    let ctx = zmq2::Context::new();

    let rep_sock = ctx.socket(zmq2::REP).expect("rep socket");
    rep_sock.bind(endpoint).expect("bind");
    let bound = rep_sock
        .get_last_endpoint()
        .expect("last_endpoint")
        .unwrap();

    // Short recv timeout lets the rep thread check `stop` on shutdown.
    rep_sock.set_rcvtimeo(100).expect("set receive_timeout");
    let reply: Vec<u8> = vec![0xEFu8; msg_size];
    let reply_for_thread = reply.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    let rep_thread = thread::spawn(move || loop {
        match rep_sock.recv_bytes(0) {
            Ok(_) => {
                if rep_sock.send(&reply_for_thread[..], 0).is_err() {
                    break;
                }
            }
            Err(zmq2::Error::EAGAIN) => {
                if stop_t.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(_) => break,
        }
    });

    let req_sock = ctx.socket(zmq2::REQ).expect("req socket");
    req_sock.connect(&bound).expect("req connect");
    thread::sleep(Duration::from_millis(50));

    let request: Vec<u8> = vec![0xCDu8; msg_size];

    b.iter(|| {
        req_sock.send(&request[..], 0).expect("req send");
        let got = req_sock.recv_bytes(0).expect("req recv");
        black_box(got);
    });

    stop.store(true, Ordering::Relaxed);
    drop(req_sock);
    rep_thread.join().ok();
}

// ── zmq.rs side ──────────────────────────────────────────────────────────────

pub fn bench_zmqrs_req_rep(c: &mut Criterion) {
    let rt = build_rt();

    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("zmqrs/req_rep/{}", transport));
        group.sample_size(30);
        group.measurement_time(Duration::from_secs(10));
        group.warm_up_time(Duration::from_secs(2));

        for &msg_size in MSG_SIZES {
            group.throughput(Throughput::Bytes(msg_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(msg_size),
                &msg_size,
                |b, &msg_size| {
                    let endpoint = match transport {
                        "tcp" => "tcp://127.0.0.1:0".to_string(),
                        "ipc" => zmqrs_ipc_path(&format!("reqrep-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_zmqrs_req_rep_one(b, &rt, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_zmqrs_req_rep_one(
    b: &mut criterion::Bencher<'_>,
    rt: &Runtime,
    msg_size: usize,
    endpoint: &str,
) {
    let (mut req, mut rep) = rt.block_on(async {
        let mut r = RepSocket::new();
        let bound = r.bind(endpoint).await.expect("rep bind").to_string();
        let mut q = ReqSocket::new();
        q.connect(bound.as_str()).await.expect("req connect");
        tokio::time::sleep(Duration::from_millis(50)).await;
        (q, r)
    });

    let request: Vec<u8> = vec![0xCDu8; msg_size];
    let reply: Vec<u8> = vec![0xEFu8; msg_size];

    b.iter(|| {
        rt.block_on(async {
            req.send(ZmqMessage::from(request.clone()))
                .await
                .expect("req send");
            black_box(rep.recv().await.expect("rep recv"));
            rep.send(ZmqMessage::from(reply.clone()))
                .await
                .expect("rep send");
            black_box(req.recv().await.expect("req recv"));
        });
    });

    drop(req);
    drop(rep);
}
