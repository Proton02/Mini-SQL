#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  // Catalog_metadata_magic_num
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  // table_meta_pages_.size()
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  // index_meta_pages_.size()
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  // table_meta_pages_
  for (auto iter : table_meta_pages_) {
    // std::map<table_id_t, page_id_t> table_meta_pages_;
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  // index_meta_pages_
  for (auto iter : index_meta_pages_) {
    // std::map<index_id_t, page_id_t> index_meta_pages_;
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    // create table_meta_page
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // Magic num + table_num + index_num +
  return 4 + 4 + 4 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */

// CatalogManager能够在数据库实例（DBStorageEngine）初次创建时（init = true）初始化元数据；
// 并在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，构建TableInfo和IndexInfo信息置于内存中。
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  Page * catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
  char* catalog_meta_page_data = catalog_meta_page->GetData();
  if(init == true) {
    catalog_meta_ = CatalogMeta::NewInstance();
    catalog_meta_->SerializeTo(catalog_meta_page_data);
  }
  else {
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page_data);
    for(auto it: catalog_meta_->table_meta_pages_) {
      LoadTable(it.first,it.second);
    }
    for(auto it: catalog_meta_->index_meta_pages_) {
      LoadIndex(it.first,it.second);
    }
  }
  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // 先检查 table_name是否已经存在
  if(table_names_.find(table_name) != table_names_.end()) {
    return DB_ALREADY_EXIST;
  }
  // 一个table包含两部分，table_info和table_heap
  // table_info 包含 table_meta 和 table_heap
  // table_meta 包含 table_id, table_name, root_page_id, schema
  // table_heap 包含 buffer_pool_manager, page_id, schema, lock_manager, log_manager
  // 创建新的table_id
  table_id_t table_id = catalog_meta_->GetNextTableId();

  // 创建新的table_meta_page
  page_id_t table_meta_page_id;
  Page *table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);

  page_id_t root_page_id;
  Page *root_page = buffer_pool_manager_->NewPage(root_page_id);
  // abort 如果没有进行DeepCopySchema
  TableSchema* schema_ = schema->DeepCopySchema(schema);
  auto table_meta = TableMetadata::Create(table_id, table_name, root_page_id , schema_);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // 创建新的table_info
  table_info = table_info->Create();
  table_info->Init(table_meta, table_heap);
  table_names_.emplace(table_name, table_id);
  tables_.emplace(table_id, table_info);

  // 更新catalog_meta_page
  catalog_meta_->table_meta_pages_.emplace(table_id, table_meta_page_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
 //question
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // tables_是一个map，key是table_id，value是table_info
  // table_names_是一个map，key是table_name，value是table_id
  // 检查指定的table_name是否存在
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  // 通过table_name找到table_id
  table_id_t table_id = table_names_[table_name];
  // 通过table_id找到table_info
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
 //question
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(tables_.empty()) {
    return DB_FAILED;
  }
  for(auto it = tables_.begin(); it != tables_.end(); it++) {
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // 1. 检查table_name是否存在
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  // 2. 检查index_name是否存在
  else {
    if(index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
      return DB_INDEX_ALREADY_EXIST;
    }
  }
  // 3. 创建新的index
  // index_meta 包含 index_id, index_name, table_id, key_map
  // index_info 包含 index_meta, table_info, buffer_pool_manager
  // indexes_ 是一个map，key是index_id，value是index_info
  // index_names_ 是一个map，key是table_name，value是index_name和index_id的map

  // 通过table_name找到table_id, table_info
  auto table_id = table_names_[table_name];
  auto table_info = tables_[table_id];

  // 创建key_map
  auto schema = table_info->GetSchema();
  vector<uint32_t>key_map;
  index_id_t  index_id;
  for(auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if(schema->GetColumnIndex(*it, index_id) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    };
    key_map.push_back(index_id);
  }
  // 创建新的index_id
  index_id = catalog_meta_->GetNextIndexId();
  // 更新index_names_
  index_names_[table_name][index_name] = index_id;
  // 创建新的index_meta_page
  page_id_t index_meta_page_id;
  Page *index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  catalog_meta_->index_meta_pages_.emplace(index_id, index_meta_page_id);
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  // 创建新的index_meta和更新meta
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_meta->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  // 创建新的index_info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);

  // 更新catalog_meta_page
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // indexes_ 是一个map，key是index_id，value是index_info
  // index_names_ 是一个map，key是table_name，value是index_name和index_id的map

  // 1. 检查table_name是否存在
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  // 2. 检查index_name是否存在
  else {
    auto map_index = index_names_.find(table_name)->second;
    if(map_index.find(index_name) == map_index.end()) {
      return DB_INDEX_NOT_FOUND;
    }
  }
  // 3. 通过table_name和index_name找到index_id
  auto index_id = index_names_.find(table_name)->second.find(index_name)->second;
  // 4. 通过index_id找到index_info
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // 1. 检查table_name是否存在
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  // index_names_ 是一个map，key是table_name，value是index_name和index_id的map
  // 2. 通过table_name找到index_name和index_id的map
  auto map_index = index_names_.at(table_name);
  // 3. 通过index_id找到index_info
  for (auto &iter : map_index) {
    index_id_t index_id = iter.second;
    auto index_info = indexes_.find(index_id)->second;
    indexes.push_back(index_info);
  }
  if (indexes.empty()) {
    return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;


}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // 1. 检查table_name是否存在
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  // 2. 通过table_name找到table_id
  // 3. 通过table_id找到table_info
  // 4. 删除table_info
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // indexes_ 是一个map，key是index_id，value是index_info
  // index_names_ 是一个map，key是table_name，value是index_name和index_id的map
  // 1. 检查table_name是否存在
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  // 2. 检查index_name是否存在
  if(index_names_.find(table_name) == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  auto map_index = index_names_.find(table_name)->second;
  if(map_index.find(index_name) == map_index.end())
    return DB_INDEX_NOT_FOUND;
  // 3. 通过table_name和index_name找到index_id
  auto index_id = map_index.find(index_name)->second;
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  // 4. 更新catalog_meta_page
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // 1. 检查table_id是否存在
  if(tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // 2. 根据page_id找到需要load的page
  Page* load_page = buffer_pool_manager_->FetchPage(page_id);
  // 3. 从page中反序列化出table_meta
  TableMetadata* table_meta_data;
  TableMetadata::DeserializeFrom(load_page->GetData(), table_meta_data);
  buffer_pool_manager_->UnpinPage(page_id, false);
  // table_meta 包含 table_id, table_name, root_page_id, schema
  // table_info 包含 table_meta, table_heap
  // table_heap 包含 buffer_pool_manager, page_id, schema, lock_manager, log_manager
  // 4. 创建新的table_heap
  auto table_heap = TableHeap::Create(buffer_pool_manager_, table_meta_data->GetSchema(), nullptr, log_manager_, lock_manager_);
  // 5. 创建新的table_info
  auto table_info = TableInfo::Create();
  table_info->Init(table_meta_data, table_heap);
  // 6. 更新tables_和table_names_
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_meta_data->GetTableName(), table_id);
  return DB_SUCCESS;
}

/**
 *
 *
 *
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // 1. 检查index_id是否存在
  if(indexes_.find(index_id) != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  // 2. 根据page_id找到需要load的page
  Page* load_page = buffer_pool_manager_->FetchPage(page_id);
  // 3. 从page中反序列化出index_meta
  IndexMetadata* index_meta_data;
  IndexMetadata::DeserializeFrom(load_page->GetData(), index_meta_data);
  buffer_pool_manager_->UnpinPage(page_id, false);
  // index_meta 包含 index_id, index_name, table_id, key_map
  // index_info 包含 index_meta, table_info, buffer_pool_manager
  IndexInfo* index_info = IndexInfo::Create();
  index_info->Init(index_meta_data, tables_[index_meta_data->GetTableId()], buffer_pool_manager_);
  // 4. 更新indexes_和index_names_
  string table_name = tables_[index_meta_data->GetTableId()]->GetTableName();
  indexes_.emplace(index_id, index_info);
  index_names_[table_name].emplace(index_meta_data->GetIndexName(), index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // 1. 检查table_id是否存在
  if(tables_.find(table_id) == tables_.end())
    return DB_TABLE_NOT_EXIST;
  // 2. 通过table_id找到table_info
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}