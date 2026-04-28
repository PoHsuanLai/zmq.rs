//! PUSH → PULL pipeline — one-way hop, no round trip.

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{prelude::*, PullSocket, PushSocket, ZmqMessage};

use crate::common::{build_rt, ipc_path, zmqrs_ipc_path, MSG_SIZES};

// ── libzmq side ──────────────────────────────────────────────────────────────

pub fn bench_push_pull(c: &mut Criterion) {
    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("libzmq/push_pull/{}", transport));
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
                        "ipc" => ipc_path(&format!("pushpull-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_libzmq_push_pull(b, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_libzmq_push_pull(b: &mut criterion::Bencher<'_>, msg_size: usize, endpoint: &str) {
    let ctx = zmq2::Context::new();
    let pull_sock = ctx.socket(zmq2::PULL).expect("pull socket");
    pull_sock.bind(endpoint).expect("bind");
    let bound = pull_sock
        .get_last_endpoint()
        .expect("last_endpoint")
        .unwrap();

    let push_sock = ctx.socket(zmq2::PUSH).expect("push socket");
    push_sock.connect(&bound).expect("push connect");
    thread::sleep(Duration::from_millis(50));

    let payload: Vec<u8> = vec![0xCDu8; msg_size];
    b.iter(|| {
        push_sock.send(&payload[..], 0).expect("push send");
        let got = pull_sock.recv_bytes(0).expect("pull recv");
        black_box(got);
    });

    drop(push_sock);
    drop(pull_sock);
}

// ── zmq.rs side ──────────────────────────────────────────────────────────────

pub fn bench_zmqrs_push_pull(c: &mut Criterion) {
    let rt = build_rt();

    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("zmqrs/push_pull/{}", transport));
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
                        "ipc" => zmqrs_ipc_path(&format!("pushpull-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_zmqrs_push_pull_one(b, &rt, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_zmqrs_push_pull_one(
    b: &mut criterion::Bencher<'_>,
    rt: &Runtime,
    msg_size: usize,
    endpoint: &str,
) {
    let (mut push, mut pull) = rt.block_on(async {
        let mut p = PullSocket::new();
        let bound = p.bind(endpoint).await.expect("pull bind").to_string();
        let mut s = PushSocket::new();
        s.connect(bound.as_str()).await.expect("push connect");
        tokio::time::sleep(Duration::from_millis(50)).await;
        (s, p)
    });

    let payload: Vec<u8> = vec![0xCDu8; msg_size];
    b.iter(|| {
        rt.block_on(async {
            push.send(ZmqMessage::from(payload.clone()))
                .await
                .expect("push send");
            black_box(pull.recv().await.expect("pull recv"));
        });
    });

    drop(push);
    drop(pull);
}
