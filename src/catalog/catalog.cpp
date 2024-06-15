#include "catalog/catalog.h"

// #define MACH_WRITE_TO(Type, Buf, Data) \
// do { \
// *reinterpret_cast<Type *>(Buf) = (Data); \
// } while (0)
// #define MACH_WRITE_UINT32(Buf, Data) MACH_WRITE_TO(uint32_t, (Buf), (Data))
// #define MACH_WRITE_INT32(Buf, Data) MACH_WRITE_TO(int32_t, (Buf), (Data))
// #define MACH_WRITE_STRING(Buf, Str)      \
// do {                                       \
// memcpy(Buf, Str.c_str(), Str.length()); \
// } while (0)
//
// #define MACH_READ_FROM(Type, Buf) (*reinterpret_cast<const Type *>(Buf))
// #define MACH_READ_UINT32(Buf) MACH_READ_FROM(uint32_t, (Buf))
// #define MACH_READ_INT32(Buf) MACH_READ_FROM(int32_t, (Buf))
//
// #define MACH_STR_SERIALIZED_SIZE(Str) (4 + Str.length())
void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
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
//    ASSERT(false, "Not Implemented yet");
  tables_.clear();
  indexes_.clear();
  table_names_.clear();
  index_names_.clear();
  Page * catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
  char* catalog_meta_page_data = catalog_meta_page->GetData();
  if(init){
    catalog_meta_=catalog_meta_->NewInstance();
    catalog_meta_->table_meta_pages_.clear();
    catalog_meta_->index_meta_pages_.clear();
    catalog_meta_->SerializeTo(catalog_meta_page_data);
  }
  else{
    catalog_meta_=catalog_meta_->DeserializeFrom(catalog_meta_page_data);
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
  table_id_t table_id=catalog_meta_->GetNextTableId();

  // 创建新的table_meta_page
  page_id_t table_meta_page_id;
  Page *table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);

  // abort 如果没有进行DeepCopySchema
  TableSchema* schema_ = schema->DeepCopySchema(schema);

  page_id_t table_page_id=0;
  // Page* table_page=buffer_pool_manager_->NewPage(table_page_id);
  TableMetadata* table_meta_=table_meta_->Create(table_id,table_name,table_page_id,schema_);
  TableHeap* table_heap_=table_heap_->Create(buffer_pool_manager_,schema_,txn,log_manager_,lock_manager_);
  table_meta_->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // 创建新的table_info
  table_info=table_info->Create();
  table_info->Init(table_meta_,table_heap_);
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
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // 1. 检查table_name是否存在
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  // 2. 检查index_name是否存在
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  // 3. 创建新的index
  // index_meta 包含 index_id, index_name, table_id, key_map
  // index_info 包含 index_meta, table_info, buffer_pool_manager
  // indexes_ 是一个map，key是index_id，value是index_info
  // index_names_ 是一个map，key是table_name，value是index_name和index_id的map

  // 通过table_name找到table_id, table_info
  auto table_id=table_names_[table_name];
  auto table_info=tables_[table_id];

  // 创建key_map
  TableSchema* schema=table_info->GetSchema();
  index_id_t  index_id;
  std::vector<uint32_t> key_map{};
  for(auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if(schema->GetColumnIndex(*it, index_id) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    };
    key_map.push_back(index_id);
  }

  // 创建新的index_id
  index_id = catalog_meta_->GetNextIndexId();
  // 创建新的index_meta_page
  page_id_t index_meta_page_id;
  Page *index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  // 创建新的index_meta和更新meta
  auto index_meta_ = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_meta_->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_page_id,true);
  // 创建新的index_info
  index_info=index_info->Create();
  index_info->Init(index_meta_,table_info,buffer_pool_manager_);

  dberr_t result = DB_SUCCESS;
  for (auto iter = table_info->GetTableHeap()->Begin(txn);iter != table_info->GetTableHeap()->End()&&result == DB_SUCCESS; ++iter) {
    Row row;
    iter->GetKeyFromRow(table_info->GetSchema(), index_info->GetIndexKeySchema(),row);
    result = index_info->GetIndex()->InsertEntry(row, iter->GetRowId(), txn);
  }
  if(result != DB_SUCCESS){
    index_info->GetIndex()->Destroy();
    buffer_pool_manager_->DeletePage(index_meta_page_id);
    delete index_info;
    return result;
  }
  // 更新index_names_, indexes_
  index_names_[table_name][index_name]=index_id;
  indexes_[index_id]=index_info;
  catalog_meta_->index_meta_pages_[index_id]=index_meta_page_id;
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
  TableInfo* table_info = tables_[table_id];
  // table删除后，需要删除table上的所有index
  for (const auto &entry : index_names_[table_name]) {
    IndexInfo *index_info = indexes_[entry.second];
    index_info->GetIndex()->Destroy();
    buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.at(entry.second));
    catalog_meta_->index_meta_pages_.erase(entry.second);
    delete index_info;
    indexes_.erase(entry.second);
  }
  index_names_.erase(table_name);
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.at(table_id));
  catalog_meta_->table_meta_pages_.erase(table_id);

  tables_.erase(table_id);
  table_names_.erase(table_name);
  table_info->~TableInfo();
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
  string table_name=table_meta_data->GetTableName();
  page_id_t table_page_id_=table_meta_data->GetFirstPageId();
  TableSchema* schema=table_meta_data->GetSchema();
  // 6. 更新tables_和table_names_
  tables_[table_id]=table_info;
  table_names_[table_name]=table_id;
  return DB_SUCCESS;
}

/**
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
  IndexMetadata* index_meta_;
  index_meta_->DeserializeFrom(load_page->GetData(),index_meta_);
  buffer_pool_manager_->UnpinPage(page_id, false);
  // index_meta 包含 index_id, index_name, table_id, key_map
  // index_info 包含 index_meta, table_info, buffer_pool_manager
  IndexInfo* index_info = IndexInfo::Create();
  string index_name=index_meta_->GetIndexName();
  index_id_t table_id=index_meta_->GetTableId();
  TableInfo* table_info=tables_[table_id];
  index_info->Init(index_meta_,table_info,buffer_pool_manager_);
  indexes_[index_id]=index_info;
  index_names_[table_info->GetTableName()][index_name]=index_id;
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
