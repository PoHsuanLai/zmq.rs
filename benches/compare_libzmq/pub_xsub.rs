//! PUB → XSUB. Application drives the SUBSCRIBE wire frame (`\x01` + topic).

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use zeromq::{prelude::*, PubSocket, XSubSocket, ZmqMessage};

use crate::common::{build_rt, ipc_path, zmqrs_ipc_path, MSG_SIZES};

// ── libzmq side ──────────────────────────────────────────────────────────────

pub fn bench_pub_xsub(c: &mut Criterion) {
    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("libzmq/pub_xsub/{}", transport));
        group.sample_size(20);
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
                        "ipc" => ipc_path(&format!("pubxsub-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_libzmq_pub_xsub(b, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_libzmq_pub_xsub(b: &mut criterion::Bencher<'_>, msg_size: usize, endpoint: &str) {
    let ctx = zmq2::Context::new();
    let pub_sock = ctx.socket(zmq2::PUB).expect("pub socket");
    pub_sock.bind(endpoint).expect("bind");
    let bound = pub_sock
        .get_last_endpoint()
        .expect("last_endpoint")
        .unwrap();

    let xsub_sock = ctx.socket(zmq2::XSUB).expect("xsub socket");
    xsub_sock.connect(&bound).expect("xsub connect");
    // Application-driven SUBSCRIBE frame: 0x01 + empty topic.
    xsub_sock.send(&[0x01u8][..], 0).expect("xsub subscribe");

    // Slow-joiner sync: PUB drops until SUBSCRIBE propagates. Blast pings
    // until one lands, then clear the timeout.
    xsub_sock.set_rcvtimeo(50).expect("set receive_timeout");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() >= deadline {
            panic!("libzmq PUB→XSUB sync did not complete within 5s");
        }
        pub_sock.send(b"sync".as_ref(), 0).expect("pub sync send");
        match xsub_sock.recv_bytes(0) {
            Ok(_) => break,
            Err(zmq2::Error::EAGAIN) => thread::sleep(Duration::from_millis(10)),
            Err(e) => panic!("xsub sync recv: {e}"),
        }
    }
    xsub_sock.set_rcvtimeo(-1).expect("clear receive_timeout");

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    b.iter(|| {
        pub_sock.send(&payload[..], 0).expect("pub send");
        let got = xsub_sock.recv_bytes(0).expect("xsub recv");
        black_box(got);
    });

    drop(pub_sock);
    drop(xsub_sock);
}

// ── zmq.rs side ──────────────────────────────────────────────────────────────

pub fn bench_zmqrs_pub_xsub(c: &mut Criterion) {
    let rt = build_rt();

    for &transport in &["tcp", "ipc"] {
        let mut group = c.benchmark_group(format!("zmqrs/pub_xsub/{}", transport));
        group.sample_size(20);
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
                        "ipc" => zmqrs_ipc_path(&format!("pubxsub-{}", msg_size)),
                        _ => unreachable!(),
                    };
                    bench_zmqrs_pub_xsub_one(b, &rt, msg_size, &endpoint);
                },
            );
        }
        group.finish();
    }
}

fn bench_zmqrs_pub_xsub_one(
    b: &mut criterion::Bencher<'_>,
    rt: &Runtime,
    msg_size: usize,
    endpoint: &str,
) {
    let (mut pub_sock, mut xsub) = rt.block_on(async {
        let mut p = PubSocket::new();
        let bound = p.bind(endpoint).await.expect("pub bind").to_string();
        let mut x = XSubSocket::new();
        x.connect(bound.as_str()).await.expect("xsub connect");
        x.send(ZmqMessage::from(vec![0x01u8]))
            .await
            .expect("xsub subscribe");

        // Slow-joiner sync — same rationale as libzmq side.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::time::Instant::now() >= deadline {
                panic!("zmq.rs PUB→XSUB sync did not complete within 5s");
            }
            p.send(ZmqMessage::from(b"sync".to_vec()))
                .await
                .expect("pub sync send");
            if tokio::time::timeout(Duration::from_millis(50), x.recv())
                .await
                .is_ok()
            {
                break;
            }
        }
        (p, x)
    });

    let payload: Vec<u8> = vec![0xABu8; msg_size];
    b.iter(|| {
        rt.block_on(async {
            pub_sock
                .send(ZmqMessage::from(payload.clone()))
                .await
                .expect("pub send");
            black_box(xsub.recv().await.expect("xsub recv"));
        });
    });

    drop(pub_sock);
    drop(xsub);
}
