//! Benchmarks for RTMP chunk parsing performance

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use axum_rtmp_server::protocol::{ChunkReader, MessageAssembler, RtmpChunk};
use bytes::Bytes;

fn create_type0_chunk(csid: u32, timestamp: u32, msg_type: u8, stream_id: u32, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Basic header
    if csid < 64 {
        buf.push(csid as u8); // fmt=0, csid
    } else if csid < 320 {
        buf.push(0);
        buf.push((csid - 64) as u8);
    } else {
        buf.push(1);
        let adjusted = csid - 64;
        buf.push(adjusted as u8);
        buf.push((adjusted >> 8) as u8);
    }

    // Message header (Type 0: 11 bytes)
    buf.push((timestamp >> 16) as u8);
    buf.push((timestamp >> 8) as u8);
    buf.push(timestamp as u8);

    let len = data.len() as u32;
    buf.push((len >> 16) as u8);
    buf.push((len >> 8) as u8);
    buf.push(len as u8);

    buf.push(msg_type);

    // Stream ID (little endian)
    buf.extend_from_slice(&stream_id.to_le_bytes());

    // Data
    buf.extend_from_slice(data);

    buf
}

fn bench_chunk_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_parsing");

    // Small chunk (typical audio)
    let small_data = vec![0u8; 128];
    let small_chunk = create_type0_chunk(8, 0, 8, 1, &small_data);
    group.throughput(Throughput::Bytes(small_chunk.len() as u64));

    group.bench_function("small_chunk_128b", |b| {
        b.iter(|| {
            let mut reader = ChunkReader::new();
            let data = Bytes::from(small_chunk.clone());
            reader.read_chunk(black_box(&data))
        })
    });

    // Medium chunk (typical video keyframe piece)
    let medium_data = vec![0u8; 4096];
    let medium_chunk = create_type0_chunk(6, 0, 9, 1, &medium_data);
    group.throughput(Throughput::Bytes(medium_chunk.len() as u64));

    group.bench_function("medium_chunk_4kb", |b| {
        b.iter(|| {
            let mut reader = ChunkReader::new();
            let data = Bytes::from(medium_chunk.clone());
            reader.read_chunk(black_box(&data))
        })
    });

    // Large chunk (max typical size)
    let large_data = vec![0u8; 65535];
    let large_chunk = create_type0_chunk(6, 0, 9, 1, &large_data);
    group.throughput(Throughput::Bytes(large_chunk.len() as u64));

    group.bench_function("large_chunk_64kb", |b| {
        b.iter(|| {
            let mut reader = ChunkReader::new();
            let data = Bytes::from(large_chunk.clone());
            reader.read_chunk(black_box(&data))
        })
    });

    group.finish();
}

fn bench_message_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_assembly");

    // Simulate assembling a fragmented message
    let total_size = 16384;
    let chunk_size = 128;
    let num_chunks = total_size / chunk_size;

    group.throughput(Throughput::Elements(num_chunks as u64));

    group.bench_function("assemble_fragmented_message", |b| {
        b.iter(|| {
            let mut assembler = MessageAssembler::new();

            for i in 0..num_chunks {
                let chunk = RtmpChunk {
                    chunk_stream_id: 6,
                    timestamp: i as u32 * 10,
                    message_type_id: 9,
                    message_stream_id: 1,
                    data: Bytes::from(vec![0u8; chunk_size]),
                };
                let _ = assembler.add_chunk(black_box(chunk), total_size as u32);
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_chunk_parsing, bench_message_assembly);
criterion_main!(benches);
