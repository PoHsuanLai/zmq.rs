//! XPUB → N SUB. XPUB exposes subscribe events on recv but is otherwise
//! a drop-in for PUB on the send side. Same fanout shape as `pub_sub`.

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{prelude::*, SubSocket, XPubSocket, ZmqMessage};

use crate::common::{build_rt, ipc_path, zmqrs_ipc_path, ArrivalStats, MSG_SIZES, SUB_COUNTS};

// ── libzmq side ──────────────────────────────────────────────────────────────

pub fn bench_xpub_sub(c: &mut Criterion) {
    for &transport in &["tcp", "ipc"] {
        for &n_subs in SUB_COUNTS {
            let mut group =
                c.benchmark_group(format!("libzmq/xpub_sub/{}/subs={}", transport, n_subs));
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
                            "ipc" => ipc_path(&format!("xpubsub-{}-{}", n_subs, msg_size)),
                            _ => unreachable!(),
                        };
                        bench_libzmq_xpub_sub(
                            b,
                            n_subs,
                            msg_size,
                            &endpoint,
                            &format!("libzmq/xpub_sub/{}/subs={}", transport, n_subs),
                            &msg_size.to_string(),
                        );
                    },
                );
            }
            group.finish();
        }
    }
}

fn bench_libzmq_xpub_sub(
    b: &mut criterion::Bencher<'_>,
    n_subs: usize,
    msg_size: usize,
    endpoint: &str,
    bench_group: &str,
    bench_input: &str,
) {
    let ctx = zmq2::Context::new();
    let xpub_sock = ctx.socket(zmq2::XPUB).expect("xpub socket");
    xpub_sock.bind(endpoint).expect("bind");
    let bound = xpub_sock
        .get_last_endpoint()
        .expect("last_endpoint")
        .unwrap();

    // Same SubHandle pattern as bench_libzmq_pub_sub.
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

    // Drain subscribe events (one per SUB) before the measured loop.
    for _ in 0..n_subs {
        let _ = xpub_sock.recv_bytes(zmq2::DONTWAIT);
    }

    // Slow-joiner sync: blast pings until every SUB has seen one.
    thread::sleep(Duration::from_millis(100));
    for s in &subs {
        s.tx_drive.send(()).expect("drive sub");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut synced = 0usize;
    while synced < n_subs {
        if std::time::Instant::now() >= deadline {
            panic!("libzmq XPUB→SUB sync did not complete within 5s");
        }
        xpub_sock.send(b"sync".as_ref(), 0).expect("xpub sync send");
        while synced < n_subs {
            match subs[synced].rx_done.recv_timeout(Duration::from_millis(10)) {
                Ok(()) => synced += 1,
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("sub thread disconnected during sync");
                }
            }
        }
    }
    while xpub_sock.recv_bytes(zmq2::DONTWAIT).is_ok() {}

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    let stats = ArrivalStats::default();
    b.iter(|| {
        for s in &subs {
            s.tx_drive.send(()).expect("drive sub");
        }
        let publish_ts = std::time::Instant::now();
        xpub_sock.send(&payload[..], 0).expect("xpub send");
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
    drop(xpub_sock);
}

// ── zmq.rs side ──────────────────────────────────────────────────────────────

pub fn bench_zmqrs_xpub_sub(c: &mut Criterion) {
    let rt = build_rt();

    for &transport in &["tcp", "ipc"] {
        for &n_subs in SUB_COUNTS {
            let mut group =
                c.benchmark_group(format!("zmqrs/xpub_sub/{}/subs={}", transport, n_subs));
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
                            "ipc" => zmqrs_ipc_path(&format!("xpubsub-{}-{}", n_subs, msg_size)),
                            _ => unreachable!(),
                        };
                        bench_zmqrs_xpub_sub_one(
                            b,
                            &rt,
                            n_subs,
                            msg_size,
                            &endpoint,
                            &format!("zmqrs/xpub_sub/{}/subs={}", transport, n_subs),
                            &msg_size.to_string(),
                        );
                    },
                );
            }
            group.finish();
        }
    }
}

fn bench_zmqrs_xpub_sub_one(
    b: &mut criterion::Bencher<'_>,
    rt: &Runtime,
    n_subs: usize,
    msg_size: usize,
    endpoint: &str,
    bench_group: &str,
    bench_input: &str,
) {
    let (mut xpub, mut subs) = rt.block_on(async {
        let mut x = XPubSocket::new();
        let bound = x.bind(endpoint).await.expect("xpub bind").to_string();
        let mut subs: Vec<SubSocket> = Vec::with_capacity(n_subs);
        for _ in 0..n_subs {
            let mut s = SubSocket::new();
            s.connect(bound.as_str()).await.expect("sub connect");
            s.subscribe("").await.expect("subscribe");
            subs.push(s);
        }

        // Drain subscribe events (one per SUB).
        for _ in 0..n_subs {
            let _ = tokio::time::timeout(Duration::from_millis(100), x.recv()).await;
        }

        // Slow-joiner sync: partition SUBs by "still waiting" until empty.
        let sync_msg = ZmqMessage::from(b"sync".to_vec());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut remaining: Vec<SubSocket> = subs;
        let mut ready: Vec<SubSocket> = Vec::with_capacity(n_subs);
        while !remaining.is_empty() {
            if std::time::Instant::now() >= deadline {
                panic!("zmq.rs XPUB→SUB sync did not complete within 5s");
            }
            x.send(sync_msg.clone()).await.expect("xpub sync send");
            let mut still_waiting = Vec::with_capacity(remaining.len());
            for mut s in remaining.drain(..) {
                match tokio::time::timeout(Duration::from_millis(5), s.recv()).await {
                    Ok(Ok(_)) => ready.push(s),
                    Ok(Err(e)) => panic!("sub sync recv error: {e:?}"),
                    Err(_) => still_waiting.push(s),
                }
            }
            remaining = still_waiting;
        }
        // Drain leftover sync bytes per SUB.
        for s in &mut ready {
            loop {
                match tokio::time::timeout(Duration::from_millis(50), s.recv()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => panic!("sub drain error: {e:?}"),
                    Err(_) => break,
                }
            }
        }
        while tokio::time::timeout(Duration::from_millis(10), x.recv())
            .await
            .is_ok()
        {}
        (x, ready)
    });

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    let stats = ArrivalStats::default();
    b.iter(|| {
        rt.block_on(async {
            let publish_ts = std::time::Instant::now();
            xpub.send(ZmqMessage::from(payload.clone()))
                .await
                .expect("xpub send");
            for s in subs.iter_mut() {
                black_box(s.recv().await.expect("sub recv"));
                let ns = publish_ts.elapsed().as_nanos() as u64;
                stats.record_per_sub(ns);
            }
            stats.record_barrier(publish_ts.elapsed().as_nanos() as u64);
        });
    });

    stats.write(bench_group, bench_input);

    drop(xpub);
    drop(subs);
}
