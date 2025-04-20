use std::sync::Arc;
use std::time::Duration;
use object_store::services::Memory;
use object_store::ObjectStore;
use mito2::access_layer::AccessLayer;
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::index::puffin_manager::PuffinManagerFactory;

pub async fn create_test_access_layer() -> Arc<AccessLayer> {
    // 创建内存对象存储
    let store = ObjectStore::new(Memory::default()).unwrap().finish();
    
    // 创建 puffin manager factory (使用临时目录)
    let puffin_manager_factory = PuffinManagerFactory::new(
        "test_aux_path",
        1024 * 1024, // 1MB staging size
        Some(1024 * 1024), // 1MB write buffer
        Some(Duration::from_secs(60)), // 60s TTL
    ).await.unwrap();

    // 创建 intermediate manager (使用临时目录)
    let intermediate_manager = IntermediateManager::init_fs("test_intermediate").await.unwrap();

    // 创建 access layer
    Arc::new(AccessLayer::new(
        "test_region",
        store,
        puffin_manager_factory,
        intermediate_manager,
    ))
} 