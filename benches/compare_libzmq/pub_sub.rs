//! PUB → N SUB fanout. Parametrized over `SUB_COUNTS`.

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{prelude::*, PubSocket, SubSocket, ZmqMessage};

use crate::common::{build_rt, ipc_path, zmqrs_ipc_path, ArrivalStats, MSG_SIZES, SUB_COUNTS};

// ── libzmq side ──────────────────────────────────────────────────────────────

pub fn bench_pub_sub(c: &mut Criterion) {
    for &transport in &["tcp", "ipc"] {
        for &n_subs in SUB_COUNTS {
            let mut group =
                c.benchmark_group(format!("libzmq/pub_sub/{}/subs={}", transport, n_subs));
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(10));
            group.warm_up_time(Duration::from_secs(2));

            for &msg_size in MSG_SIZES {
                group.throughput(Throughput::Bytes((msg_size as u64) * (n_subs as u64)));
                group.bench_with_input(
                    BenchmarkId::from_parameter(msg_size),
                    &msg_size,
                    |b, &msg_size| {
                        let endpoint = match transport {
                            "tcp" => "tcp://127.0.0.1:0".to_string(),
                            "ipc" => ipc_path(&format!("pubsub-{}-{}", n_subs, msg_size)),
                            _ => unreachable!(),
                        };
                        bench_libzmq_pub_sub(
                            b,
                            n_subs,
                            msg_size,
                            &endpoint,
                            &format!("libzmq/pub_sub/{}/subs={}", transport, n_subs),
                            &msg_size.to_string(),
                        );
                    },
                );
            }
            group.finish();
        }
    }
}

fn bench_libzmq_pub_sub(
    b: &mut criterion::Bencher<'_>,
    n_subs: usize,
    msg_size: usize,
    endpoint: &str,
    bench_group: &str,
    bench_input: &str,
) {
    let ctx = zmq2::Context::new();

    let pub_sock = ctx.socket(zmq2::PUB).expect("pub socket");
    pub_sock.bind(endpoint).expect("bind");
    let bound = pub_sock
        .get_last_endpoint()
        .expect("last_endpoint")
        .unwrap();

    // Each subscriber runs on its own OS thread (libzmq sockets aren't
    // thread-safe), driven by a pair of channels per iter.
    struct SubHandle {
        tx_drive: mpsc::Sender<()>,
        rx_done: mpsc::Receiver<()>,
        _thread: thread::JoinHandle<()>,
    }

    let mut subs: Vec<SubHandle> = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let ctx2 = ctx.clone();
        let bound2 = bound.clone();
        let (tx_drive, rx_drive) = mpsc::channel::<()>();
        let (tx_done, rx_done) = mpsc::channel::<()>();
        let t = thread::spawn(move || {
            let sub = ctx2.socket(zmq2::SUB).expect("sub socket");
            sub.connect(&bound2).expect("sub connect");
            sub.set_subscribe(b"").expect("subscribe");
            tx_done.send(()).ok();
            while rx_drive.recv().is_ok() {
                let _ = sub.recv_bytes(0).expect("sub recv");
                tx_done.send(()).ok();
            }
        });
        rx_done.recv().expect("sub ready");
        subs.push(SubHandle {
            tx_drive,
            rx_done,
            _thread: t,
        });
    }

    thread::sleep(Duration::from_millis(100));

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    let stats = ArrivalStats::default();

    b.iter(|| {
        for s in &subs {
            s.tx_drive.send(()).expect("drive sub");
        }
        let publish_ts = std::time::Instant::now();
        pub_sock.send(&payload[..], 0).expect("pub send");
        for s in &subs {
            s.rx_done.recv().expect("sub done");
            let ns = publish_ts.elapsed().as_nanos() as u64;
            stats.record_per_sub(ns);
        }
        stats.record_barrier(publish_ts.elapsed().as_nanos() as u64);
        black_box(&payload);
    });

    stats.write(bench_group, bench_input);

    for s in subs {
        drop(s.tx_drive);
        drop(s.rx_done);
    }
}

// ── zmq.rs side ──────────────────────────────────────────────────────────────

pub fn bench_zmqrs_pub_sub(c: &mut Criterion) {
    let rt = build_rt();

    for &transport in &["tcp", "ipc"] {
        for &n_subs in SUB_COUNTS {
            let mut group =
                c.benchmark_group(format!("zmqrs/pub_sub/{}/subs={}", transport, n_subs));
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(10));
            group.warm_up_time(Duration::from_secs(2));

            for &msg_size in MSG_SIZES {
                group.throughput(Throughput::Bytes((msg_size as u64) * (n_subs as u64)));
                group.bench_with_input(
                    BenchmarkId::from_parameter(msg_size),
                    &msg_size,
                    |b, &msg_size| {
                        let endpoint = match transport {
                            "tcp" => "tcp://127.0.0.1:0".to_string(),
                            "ipc" => zmqrs_ipc_path(&format!("pubsub-{}-{}", n_subs, msg_size)),
                            _ => unreachable!(),
                        };
                        bench_zmqrs_pub_sub_one(
                            b,
                            &rt,
                            n_subs,
                            msg_size,
                            &endpoint,
                            &format!("zmqrs/pub_sub/{}/subs={}", transport, n_subs),
                            &msg_size.to_string(),
                        );
                    },
                );
            }
            group.finish();
        }
    }
}

fn bench_zmqrs_pub_sub_one(
    b: &mut criterion::Bencher<'_>,
    rt: &Runtime,
    n_subs: usize,
    msg_size: usize,
    endpoint: &str,
    bench_group: &str,
    bench_input: &str,
) {
    let (mut pub_sock, mut subs) = rt.block_on(async {
        let mut p = PubSocket::new();
        let bound = p.bind(endpoint).await.expect("pub bind").to_string();
        let mut subs: Vec<SubSocket> = Vec::with_capacity(n_subs);
        for _ in 0..n_subs {
            let mut s = SubSocket::new();
            s.connect(bound.as_str()).await.expect("sub connect");
            s.subscribe("").await.expect("subscribe");
            subs.push(s);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        (p, subs)
    });

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    let stats = ArrivalStats::default();

    b.iter(|| {
        rt.block_on(async {
            let publish_ts = std::time::Instant::now();
            pub_sock
                .send(ZmqMessage::from(payload.clone()))
                .await
                .expect("pub send");
            for s in subs.iter_mut() {
                black_box(s.recv().await.expect("sub recv"));
                let ns = publish_ts.elapsed().as_nanos() as u64;
                stats.record_per_sub(ns);
            }
            stats.record_barrier(publish_ts.elapsed().as_nanos() as u64);
        });
    });

    stats.write(bench_group, bench_input);

    drop(subs);
    drop(pub_sock);
}
