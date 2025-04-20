use std::sync::Arc;
use std::time::{Duration, Instant};
use mito2::sst::file::{FileId, FileTimeRange};
use mito2::sst::parquet::writer::ParquetWriter;
use mito2::test_util::memtable_util::{self, region_metadata_to_row_schema};
use mito2::memtable::{KeyValues, Memtable};
use mito2::memtable::time_series::TimeSeriesMemtable;
use mito2::region::options::{MergeMode, IndexOptions};
use mito2::row_converter::DensePrimaryKeyCodec;
use mito2::test_util::sst_util::{sst_file_handle, new_batch_by_range, new_source, sst_region_metadata};
use mito2::test_util::{check_reader_result, TestEnv};
use mito2::access_layer::FilePathProvider;
use common_base::readable_size::ReadableSize;
use parquet::file::metadata::ParquetMetaData;
use mito2::sst::index::IndexOutput;
use mito2::sst::parquet::WriteOptions;
// use mito2::sst::parquet::{FixedPathProvider, SstInfo};

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use criterion::async_executor::FuturesExecutor;

mod memory_store;



use mito2::sst::{location, DEFAULT_WRITE_CONCURRENCY, DEFAULT_WRITE_BUFFER_SIZE};
use mito2::sst::index::{Indexer, IndexerBuilder};

pub(crate) const DEFAULT_READ_BATCH_SIZE: usize = 1024;
/// Default row group size for parquet files.
pub(crate) const DEFAULT_ROW_GROUP_SIZE: usize = 100 * DEFAULT_READ_BATCH_SIZE;


const FILE_DIR: &str = "/";

#[derive(Clone)]
struct FixedPathProvider {
    file_id: FileId,
}

impl FilePathProvider for FixedPathProvider {
    fn build_index_file_path(&self, _file_id: FileId) -> String {
        location::index_file_path(FILE_DIR, self.file_id)
    }

    fn build_sst_file_path(&self, _file_id: FileId) -> String {
        location::sst_file_path(FILE_DIR, self.file_id)
    }
}

struct NoopIndexBuilder;

#[async_trait::async_trait]
impl IndexerBuilder for NoopIndexBuilder {
    async fn build(&self, _file_id: FileId) -> Indexer {
        Indexer::default()
    }
}

#[derive(Debug)]
pub struct SstInfo {
    /// SST file id.
    pub file_id: FileId,
    /// Time range of the SST. The timestamps have the same time unit as the
    /// data in the SST.
    pub time_range: FileTimeRange,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of rows.
    pub num_rows: usize,
    /// Number of row groups
    pub num_row_groups: u64,
    /// File Meta Data
    pub file_metadata: Option<Arc<ParquetMetaData>>,
    /// Index Meta Data
    pub index_metadata: IndexOutput,
}




async fn do_something(size: usize) {
    let mut env = TestEnv::new();
    let handle = sst_file_handle(0, 1000);
    let file_path = FixedPathProvider {
        file_id: handle.file_id(),
    };
    let object_store = env.init_object_store_manager();
    let metadata = Arc::new(sst_region_metadata());

    let mut writer = ParquetWriter::new_with_object_store(
        object_store.clone(),
        metadata.clone(),
        NoopIndexBuilder,
        file_path,
    ).await;
    let source = new_source(&[
        new_batch_by_range(&["a", "d"], 0, size),
        new_batch_by_range(&["b", "f"], 0, size),
        new_batch_by_range(&["b", "h"], 0, size),
    ]);
    let write_opts = WriteOptions {
        row_group_size: 50,
        ..Default::default()
    };

    let info = writer.write_all(source, None, &write_opts).await.unwrap();
}

fn larger(c: &mut Criterion) {

}

fn from_elem(c: &mut Criterion) {
    let size: usize = 100;

    c.bench_with_input(BenchmarkId::new("input_example", size), &size, |b, &s| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| do_something(s));
    });

    c.bench_function("larger", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();
            let metadata = Arc::new(sst_region_metadata());

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                NoopIndexBuilder,
                file_path,
            ).await;
            let source = new_source(&[
                new_batch_by_range(&["a", "d"], 0, 100000),
                new_batch_by_range(&["b", "f"], 0, 100000),
                new_batch_by_range(&["b", "h"], 0, 100000),
            ]);
            let write_opts = WriteOptions {
                row_group_size: 100,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap().remove(0);
        });
    });

    c.bench_function("larger-key", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();
            let metadata = Arc::new(sst_region_metadata());

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                NoopIndexBuilder,
                file_path,
            ).await;
            let source = new_source(&[
                new_batch_by_range(&["a", "d", "yijun"], 0, 100000),
                new_batch_by_range(&["b", "f", "yuang"], 0, 100000),
                new_batch_by_range(&["b", "h", "voddle"], 0, 100000),
            ]);
            let write_opts = WriteOptions {
                row_group_size: 100,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap().remove(0);
        });
    });

}



criterion_group!(benches, from_elem);
criterion_main!(benches);


//}

// pub fn sst_write_benchmark(c: &mut Criterion) {
//     let mut group = c.benchmark_group("sst_write");
//     group.throughput(criterion::Throughput::Elements(1u64));
//     group.sample_size(10);
// 
//     let rt = tokio::runtime::Runtime::new().unwrap();
//     rt.block_on(async {
//         let access_layer = memory_store::create_test_access_layer().await;
//         let metadata = Arc::new(sst_region_metadata());
//         let timestamps = (0..1000).collect::<Vec<_>>();
//         let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
// 
//         let factory = ObjectStoreWriterFactory::create("test_region".to_string());
//         let handle = sst_file_handle(0, 1000);
//         let file_path = FixedPathProvider {
//             file_id: handle.file_id(),
//         };
//         let path_provider = RegionFilePathFactory::new("test_region".to_string());
//         let object_store = env.init_object_store_manager();
// 
//         // 添加不同大小的测试
//         let sizes = [100, 1000, 10000, 100000];
//         for size in sizes.iter() {
//             group.bench_with_input(BenchmarkId::new("sst_write", size), size, |b, &size| {
//                 b.iter(|| {
//                     let file_id = FileId::random();
//                     let mut writer = ParquetWriter::new_with_object_store(
//                         object_store.clone(),
//                         metadata.clone(),
//                         NoopIndexBuilder,
//                         file_path,
//                     );
// 
//                     let memtable = TimeSeriesMemtable::new(
//                         metadata.clone(),
//                         1,
//                         None,
//                         true,
//                         MergeMode::LastRow,
//                     );
//                     
//                     memtable.write(&kvs).unwrap();
//                     writer.write_memtable(&memtable).unwrap();
//                     writer.finish().unwrap();
//                 })
//             });
//         }
//     });
// }

// 添加内存测量
// struct MemoryMeasurement;
// impl criterion::measurement::Measurement for MemoryMeasurement {
//     type Intermediate = usize;
//     type Value = usize;
// 
//     fn start(&self) -> Self::Intermediate { 0 }
//     fn end(&self, i: Self::Intermediate) -> Self::Value { i }
//     fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value { v1 + v2 }
//     fn zero(&self) -> Self::Value { 0 }
//     fn to_f64(&self, value: &Self::Value) -> f64 { *value as f64 }
//     fn formatter(&self) -> &dyn criterion::measurement::ValueFormatter {
//         &criterion::measurement::SizeFormatter
//     }
// }
// 
// criterion_group!(benches, sst_write_benchmark);
// criterion_main!(benches);