use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};
use mito2::sst::file::{FileId, FileTimeRange};
use mito2::sst::parquet::writer::ParquetWriter;
use mito2::test_util::sst_util::{sst_file_handle, new_batch_by_range, new_source, sst_region_metadata};
use mito2::test_util::{TestEnv};
use mito2::access_layer::FilePathProvider;
use parquet::file::metadata::ParquetMetaData;
use mito2::sst::index::IndexOutput;
use mito2::sst::parquet::WriteOptions;
use mito2::sst::index::IndexerBuilderImpl;
use mito2::region::options::IndexOptions;
use mito2::config::{IndexConfig, InvertedIndexConfig, FulltextIndexConfig, BloomFilterConfig};
use mito2::sst::index::puffin_manager::PuffinManagerFactory;
use mito2::sst::index::intermediate::IntermediateManager;
use object_store::ObjectStore;
use object_store::services::Memory;
use mito2::access_layer::OperationType;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, RegionId};
use datatypes::schema::{
    ColumnSchema, FulltextOptions, SkippingIndexOptions, SkippingIndexType,
};
use datatypes::data_type::ConcreteDataType;
use api::v1::SemanticType;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use mito2::read::BatchColumn;
use mito2::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
use mito2::row_converter::SortField;
use datatypes::value::ValueRef;
use datatypes::vectors::{UInt64Vector, UInt8Vector, TimestampMillisecondVector};
use mito2::read::Batch;
use std::iter;
// use mito2::sst::parquet::{FixedPathProvider, SstInfo};

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

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

struct TestFilePathProvider;

impl FilePathProvider for TestFilePathProvider {
    fn build_index_file_path(&self, file_id: FileId) -> String {
        file_id.to_string()
    }

    fn build_sst_file_path(&self, file_id: FileId) -> String {
        file_id.to_string()
    }
}

struct NoopIndexBuilder;

#[async_trait::async_trait]
impl IndexerBuilder for NoopIndexBuilder {
    async fn build(&self, _file_id: FileId) -> Indexer {
        Indexer::default()
    }
}

async fn mock_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
    IntermediateManager::init_fs(path).await.unwrap()
}

fn mock_object_store() -> ObjectStore {
    ObjectStore::new(Memory::default()).unwrap().finish()
}

struct NoopPathProvider;

impl FilePathProvider for NoopPathProvider {
    fn build_index_file_path(&self, _file_id: FileId) -> String {
        unreachable!()
    }

    fn build_sst_file_path(&self, _file_id: FileId) -> String {
        unreachable!()
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

struct MetaConfig {
    with_inverted: bool,
    with_fulltext: bool,
    with_skipping_bloom: bool,
}

fn mock_region_metadata(
    MetaConfig {
        with_inverted,
        with_fulltext,
        with_skipping_bloom,
    }: MetaConfig,
) -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
    let mut column_schema = ColumnSchema::new("tag_str", ConcreteDataType::string_datatype(), false);
    if with_inverted {
        column_schema = column_schema.with_inverted_index(true);
    }
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: column_schema
            .with_skipping_options(SkippingIndexOptions {
                index_type: SkippingIndexType::BloomFilter,
                granularity: 50,
            })
            .unwrap(),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts", 
                ConcreteDataType::timestamp_millisecond_datatype(), 
                false),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "field_u64",
                ConcreteDataType::uint64_datatype(),
                false,
            )
            .with_skipping_options(SkippingIndexOptions {
                index_type: SkippingIndexType::BloomFilter,
                granularity: 50,
            })
            .unwrap(),
            semantic_type: SemanticType::Field,
            column_id: 3,
        })
        .primary_key(vec![1]);

    if with_fulltext {
        let column_schema =
            ColumnSchema::new("text", ConcreteDataType::string_datatype(), true)
                .with_fulltext_options(FulltextOptions {
                    enable: true,
                    ..Default::default()
                })
                .unwrap();

        let column = ColumnMetadata {
            column_schema,
            semantic_type: SemanticType::Field,
            column_id: 4,
        };

        builder.push_column_metadata(column);
    }

    if with_skipping_bloom {
        let column_schema =
            ColumnSchema::new("bloom", ConcreteDataType::string_datatype(), false)
                .with_skipping_options(SkippingIndexOptions {
                    granularity: 42,
                    index_type: SkippingIndexType::BloomFilter,
                })
                .unwrap();

        let column = ColumnMetadata {
            column_schema,
            semantic_type: SemanticType::Field,
            column_id: 5,
        };

        builder.push_column_metadata(column);
    }

    Arc::new(builder.build().unwrap())
}

pub fn new_batch(str_tag: impl AsRef<str>, u64_field: impl IntoIterator<Item = u64>) -> Batch {
    let fields = vec![(0, SortField::new(ConcreteDataType::string_datatype()))];
    let codec = DensePrimaryKeyCodec::with_fields(fields);
    let row: [ValueRef; 1] = [str_tag.as_ref().into()];
    let primary_key = codec.encode(row.into_iter()).unwrap();

    let u64_field = BatchColumn {
        column_id: 3,
        data: Arc::new(UInt64Vector::from_iter_values(u64_field)),
    };
    let num_rows = u64_field.data.len();

    Batch::new(
        primary_key,
        Arc::new(TimestampMillisecondVector::from_iter_values(
            iter::repeat(0).take(num_rows),
        )),
        Arc::new(UInt64Vector::from_iter_values(
            iter::repeat(0).take(num_rows),
        )),
        Arc::new(UInt8Vector::from_iter_values(
            iter::repeat(1).take(num_rows),
        )),
        vec![u64_field],
    )
    .unwrap()
}


async fn do_something(size: usize) {
    let mut env = TestEnv::new();
    let handle = sst_file_handle(0, 1000);
    let file_path = FixedPathProvider {
        file_id: handle.file_id(),
    };
    let object_store = env.init_object_store_manager();

    let (dir, factory) =
        PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
    let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

    let metadata = mock_region_metadata(MetaConfig {
        with_inverted: false,
        with_fulltext: false,
        with_skipping_bloom: false,
    });


    let indexer = IndexerBuilderImpl {
        op_type: OperationType::Flush,
        metadata: metadata.clone(),
        row_group_size: 1024,
        puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
        intermediate_manager: intm_manager,
        index_options: IndexOptions::default(),
        inverted_index_config: InvertedIndexConfig::default(),
        fulltext_index_config: FulltextIndexConfig::default(),
        bloom_filter_index_config: BloomFilterConfig::default(),
    };

    let mut writer = ParquetWriter::new_with_object_store(
        object_store.clone(),
        metadata.clone(),
        indexer,
        file_path,
    ).await;

    // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
    let mut batch0 = new_batch("tag1", 0..10);
    let mut batch1 = new_batch("tag1", 0..10);
    let mut batch2 = new_batch("tag2", 0..10000);
    
    let source = new_source(&[batch0, batch1, batch2]);
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
    let mut group = c.benchmark_group("larger");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    group.bench_with_input(BenchmarkId::new("simple", size), &size, |b, &s| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| do_something(s));
    });

    group.bench_function("nothing", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;
            writer.type_id();
        });
    });


    group.bench_function("large", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("tag1", 0..10000);
            let mut batch1 = new_batch("tag1", 0..10000);
            let mut batch2 = new_batch("tag2", 0..10000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 50,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();

        });
    });



    group.bench_function("larger", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("tag1", 0..100000);
            let mut batch1 = new_batch("tag1", 0..100000);
            let mut batch2 = new_batch("tag2", 0..100000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 50,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();

        });
    });

    group.bench_function("small-row-group", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("tag1", 0..100000);
            let mut batch1 = new_batch("tag1", 0..100000);
            let mut batch2 = new_batch("tag2", 0..100000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 10,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();
        });
    });

    group.bench_function("large-row-group", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("tag1", 0..100000);
            let mut batch1 = new_batch("tag1", 0..100000);
            let mut batch2 = new_batch("tag2", 0..100000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 1000,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();
        });
    });



    group.bench_function("less-larger-key", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("voddle", 0..1000);
            let mut batch1 = new_batch("voddle", 0..1000);
            let mut batch2 = new_batch("IamSoTired", 0..1000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 50,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();

        });
    });


    group.bench_function("more-larger-key", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async move {
            let mut env = TestEnv::new();
            let handle = sst_file_handle(0, 1000);
            let file_path = FixedPathProvider {
                file_id: handle.file_id(),
            };
            let object_store = env.init_object_store_manager();

            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
            let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

            let metadata = mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            });


            let indexer = IndexerBuilderImpl {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                row_group_size: 1024,
                puffin_manager: factory.build(mock_object_store(), TestFilePathProvider),
                intermediate_manager: intm_manager,
                index_options: IndexOptions::default(),
                inverted_index_config: InvertedIndexConfig::default(),
                fulltext_index_config: FulltextIndexConfig::default(),
                bloom_filter_index_config: BloomFilterConfig::default(),
            };

            let mut writer = ParquetWriter::new_with_object_store(
                object_store.clone(),
                metadata.clone(),
                indexer,
                file_path,
            ).await;

            // writer.current_indexer.unwrap().bloom_filter_indexer = Some(bloom_indexer);
            let mut batch0 = new_batch("voddle", 0..100000);
            let mut batch1 = new_batch("voddle", 0..100000);
            let mut batch2 = new_batch("IamSoTired", 0..100000);
            
            let source = new_source(&[batch0, batch1, batch2]);
            let write_opts = WriteOptions {
                row_group_size: 50,
                ..Default::default()
            };

            let info = writer.write_all(source, None, &write_opts).await.unwrap();

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