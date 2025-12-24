//! Benchmarks for AMF0 encoding/decoding performance

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use axum_rtmp_server::amf::{Amf0Codec, AmfValue};
use std::collections::HashMap;

fn bench_amf_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("amf_encoding");

    let codec = Amf0Codec::new();

    // Benchmark number encoding
    group.bench_function("encode_number", |b| {
        b.iter(|| {
            codec.encode(&AmfValue::Number(black_box(1234.5678)))
        })
    });

    // Benchmark string encoding
    let test_string = "This is a typical RTMP string value for testing";
    group.bench_function("encode_string", |b| {
        b.iter(|| {
            codec.encode(&AmfValue::String(black_box(test_string.to_string())))
        })
    });

    // Benchmark object encoding (typical connect command)
    let mut connect_obj = HashMap::new();
    connect_obj.insert("app".to_string(), AmfValue::String("live".to_string()));
    connect_obj.insert("flashVer".to_string(), AmfValue::String("FMLE/3.0 (compatible; FMSc/1.0)".to_string()));
    connect_obj.insert("tcUrl".to_string(), AmfValue::String("rtmp://localhost:1935/live".to_string()));
    connect_obj.insert("fpad".to_string(), AmfValue::Boolean(false));
    connect_obj.insert("capabilities".to_string(), AmfValue::Number(239.0));
    connect_obj.insert("audioCodecs".to_string(), AmfValue::Number(3575.0));
    connect_obj.insert("videoCodecs".to_string(), AmfValue::Number(252.0));
    connect_obj.insert("videoFunction".to_string(), AmfValue::Number(1.0));

    group.bench_function("encode_connect_object", |b| {
        b.iter(|| {
            codec.encode(&AmfValue::Object(black_box(connect_obj.clone())))
        })
    });

    // Benchmark array encoding (metadata)
    let metadata = vec![
        AmfValue::String("width".to_string()),
        AmfValue::Number(1920.0),
        AmfValue::String("height".to_string()),
        AmfValue::Number(1080.0),
        AmfValue::String("framerate".to_string()),
        AmfValue::Number(30.0),
        AmfValue::String("videodatarate".to_string()),
        AmfValue::Number(6000.0),
        AmfValue::String("audiodatarate".to_string()),
        AmfValue::Number(128.0),
    ];

    group.bench_function("encode_metadata_array", |b| {
        b.iter(|| {
            codec.encode(&AmfValue::StrictArray(black_box(metadata.clone())))
        })
    });

    group.finish();
}

fn bench_amf_decoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("amf_decoding");

    let codec = Amf0Codec::new();

    // Prepare encoded data
    let encoded_number = codec.encode(&AmfValue::Number(1234.5678));
    group.throughput(Throughput::Bytes(encoded_number.len() as u64));

    group.bench_function("decode_number", |b| {
        b.iter(|| {
            codec.decode(black_box(&encoded_number))
        })
    });

    let encoded_string = codec.encode(&AmfValue::String("This is a typical RTMP string value".to_string()));
    group.throughput(Throughput::Bytes(encoded_string.len() as u64));

    group.bench_function("decode_string", |b| {
        b.iter(|| {
            codec.decode(black_box(&encoded_string))
        })
    });

    // Encode a complex object for decoding benchmark
    let mut obj = HashMap::new();
    obj.insert("app".to_string(), AmfValue::String("live".to_string()));
    obj.insert("tcUrl".to_string(), AmfValue::String("rtmp://localhost:1935/live".to_string()));
    obj.insert("flashVer".to_string(), AmfValue::String("FMLE/3.0".to_string()));
    obj.insert("capabilities".to_string(), AmfValue::Number(239.0));

    let encoded_object = codec.encode(&AmfValue::Object(obj));
    group.throughput(Throughput::Bytes(encoded_object.len() as u64));

    group.bench_function("decode_object", |b| {
        b.iter(|| {
            codec.decode(black_box(&encoded_object))
        })
    });

    group.finish();
}

fn bench_amf_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("amf_roundtrip");

    let codec = Amf0Codec::new();

    // Typical RTMP command roundtrip
    let mut connect_obj = HashMap::new();
    connect_obj.insert("app".to_string(), AmfValue::String("live".to_string()));
    connect_obj.insert("tcUrl".to_string(), AmfValue::String("rtmp://localhost:1935/live".to_string()));
    connect_obj.insert("flashVer".to_string(), AmfValue::String("FMLE/3.0 (compatible; FMSc/1.0)".to_string()));
    connect_obj.insert("fpad".to_string(), AmfValue::Boolean(false));
    connect_obj.insert("capabilities".to_string(), AmfValue::Number(239.0));

    let original = AmfValue::Object(connect_obj);

    group.bench_function("encode_decode_connect", |b| {
        b.iter(|| {
            let encoded = codec.encode(&original);
            let (decoded, _) = codec.decode(black_box(&encoded)).unwrap();
            decoded
        })
    });

    group.finish();
}

criterion_group!(benches, bench_amf_encoding, bench_amf_decoding, bench_amf_roundtrip);
criterion_main!(benches);
