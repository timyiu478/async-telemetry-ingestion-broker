use async_telemetry_broker::{
    config::{Config, WorkerConfig},
    downstream,
    frame::Frame,
    server,
};
use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const NUM_CONCURRENT_CLIENTS: usize = 40;
const FRAMES_PER_CLIENT: usize = 10_000;
const PAYLOAD_SIZE_BYTES: usize = 1024;

/// Robustly writes to a TCP stream, automatically backing off if macOS returns
/// `ENOBUFS` (OS Error 55: No buffer space available).
async fn write_frame_robust(stream: &mut TcpStream, frame: &[u8]) -> std::io::Result<()> {
    loop {
        match stream.write_all(frame).await {
            Ok(()) => return Ok(()),
            // OS Error 55 = ENOBUFS on macOS / BSD
            Err(err) if err.raw_os_error() == Some(55) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("async_telemetry_broker=warn")
        .init();

    println!("Starting Async Telemetry Broker Benchmark...");
    println!("Clients: {}", NUM_CONCURRENT_CLIENTS);
    println!("Frames per client: {}", FRAMES_PER_CLIENT);
    println!("Payload size: {} bytes", PAYLOAD_SIZE_BYTES);

    let addr: SocketAddr = "127.0.0.1:7778".parse().unwrap();
    let config = Config::default()
        .with_listen_addr(addr)
        .with_broadcast_capacity(1024);

    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let (tx, _) = broadcast::channel::<Frame>(config.broadcast_capacity);

    // Spawn slow downstream worker (50ms delay)
    let slow_worker = WorkerConfig {
        id: 1,
        processing_delay: Duration::from_millis(50),
    };
    let rx = tx.subscribe();
    let cancel_worker = cancel.clone();
    tracker.spawn(async move {
        downstream::run_worker(slow_worker, rx, cancel_worker).await;
    });

    // Spawn TCP Broker
    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    let tx_srv = tx.clone();
    let srv_config = config.clone();
    tokio::spawn(async move {
        server::run(srv_config, tx_srv, srv_tracker, srv_cancel).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Pre-compute raw binary frame
    let mut buf = BytesMut::with_capacity(4 + PAYLOAD_SIZE_BYTES);
    buf.put_u32(PAYLOAD_SIZE_BYTES as u32);
    buf.put_slice(&vec![0u8; PAYLOAD_SIZE_BYTES]);
    let raw_frame = buf.freeze();

    let cancel_clients = cancel.clone();

    // Isolated runtime thread for load generation
    std::thread::spawn(move || {
        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        client_rt.block_on(async {
            println!("Connecting clients...");
            let mut streams = Vec::new();
            for _ in 0..NUM_CONCURRENT_CLIENTS {
                let stream = TcpStream::connect(addr).await.expect("Failed to connect");
                stream.set_nodelay(true).unwrap();
                streams.push(stream);
            }

            println!("Warming up...");
            for stream in &mut streams {
                for _ in 0..100 {
                    write_frame_robust(stream, &raw_frame).await.unwrap();
                }
            }
            
            tokio::time::sleep(Duration::from_millis(500)).await;

            println!("Starting measured load...\n");
            let start_time = Instant::now();
            let mut join_set = tokio::task::JoinSet::new();

            for mut stream in streams {
                let frame_data = raw_frame.clone();
                join_set.spawn(async move {
                    for _ in 0..FRAMES_PER_CLIENT {
                        write_frame_robust(&mut stream, &frame_data).await.unwrap();
                    }
                    stream.flush().await.unwrap();
                });
            }

            while let Some(res) = join_set.join_next().await {
                res.expect("Client task panicked");
            }

            let elapsed = start_time.elapsed();
            
            let total_frames = NUM_CONCURRENT_CLIENTS * FRAMES_PER_CLIENT;
            let total_bytes = total_frames * (4 + PAYLOAD_SIZE_BYTES);
            let total_mb = total_bytes as f64 / 1_048_576.0;
            
            let frames_per_sec = total_frames as f64 / elapsed.as_secs_f64();
            let mb_per_sec = total_mb / elapsed.as_secs_f64();

            println!("=== Benchmark Results ===");
            println!("Time Elapsed:  {:.2?}", elapsed);
            println!("Total Data:    {:.2} MB ({} frames)", total_mb, total_frames);
            println!("Throughput:    {:.2} MB/s", mb_per_sec);
            println!("Frame Rate:    {:.0} frames/sec", frames_per_sec);
            println!("=========================");

            cancel_clients.cancel();
        });
    }).join().unwrap();

    tracker.close();
    tracker.wait().await;
}
