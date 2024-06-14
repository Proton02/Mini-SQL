#include "catalog/catalog.h"

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
  //ASSERT(false, "Not Implemented yet");
  return sizeof(CATALOG_METADATA_MAGIC_NUM)+sizeof(table_meta_pages_.size())+sizeof(index_meta_pages_.size())+table_meta_pages_.size()*(sizeof(table_id_t)+sizeof(index_id_t))+index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(table_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  tables_.clear();
  indexes_.clear();
  table_names_.clear();
  index_names_.clear();
  Page* page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char* temp = page->GetData();
  if(init){
    catalog_meta_=catalog_meta_->NewInstance();
    catalog_meta_->table_meta_pages_.clear();
    catalog_meta_->index_meta_pages_.clear();
    catalog_meta_->SerializeTo(temp);
  }
  else{
    catalog_meta_=catalog_meta_->DeserializeFrom(temp);
    for(std::pair <table_id_t, page_id_t> i : catalog_meta_->table_meta_pages_ ){
      LoadTable(i.first,i.second);
    }
    for(std::pair <table_id_t, page_id_t> i : catalog_meta_->index_meta_pages_ ){
      LoadIndex(i.first,i.second);
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
  //return DB_FAILED;
  auto temp=table_names_.find(table_name);
  if(temp!=table_names_.end()){
    return DB_TABLE_ALREADY_EXIST;
  }
  table_info=table_info->Create();
  table_id_t table_id=catalog_meta_->GetNextTableId();
  TableSchema* schema_=schema_->DeepCopySchema(schema);
  page_id_t meta_page_id=0;
  Page* meta_page=buffer_pool_manager_->NewPage(meta_page_id);
  page_id_t table_page_id=0;
  // Page* table_page=buffer_pool_manager_->NewPage(table_page_id);
  TableMetadata* table_meta_=table_meta_->Create(table_id,table_name,table_page_id,schema_);
  table_meta_->SerializeTo(meta_page->GetData());
  TableHeap* table_heap_=table_heap_->Create(buffer_pool_manager_,schema_,txn,log_manager_,lock_manager_);
  table_info->Init(table_meta_,table_heap_);
  table_names_[table_name]=table_id;
  tables_[table_id]=table_info;

  catalog_meta_->table_meta_pages_[table_id]=meta_page_id;
  Page* page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char* temp1=page->GetData();
  catalog_meta_->SerializeTo(temp1);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto temp=table_names_.find(table_name);
  if(temp==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info=tables_[temp->second];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  for(auto i:tables_){
    tables.push_back(i.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto temp=table_names_.find(table_name);
  if(temp==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto temp1=index_names_.find(table_name);
  if(temp1!=index_names_.end()&&temp1->second.find(index_name)!=temp1->second.end()){
    return DB_INDEX_ALREADY_EXIST;
  }
  index_info=index_info->Create();
  index_id_t index_id=catalog_meta_->GetNextIndexId();
  table_id_t table_id=table_names_[table_name];
  TableInfo* table_info=tables_[table_id];
  TableSchema* schema=table_info->GetSchema();
  std::vector<uint32_t> key_map{};
  for(auto i:index_keys){
    uint32_t index;
    auto temp3=schema->GetColumnIndex(i,index);
    if(temp3==DB_COLUMN_NAME_NOT_EXIST){
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(index);
  }
  page_id_t meta_page_id=0;
  Page* meta_page=buffer_pool_manager_->NewPage(meta_page_id);
  IndexMetadata* index_meta_=nullptr;
  index_meta_=index_meta_->Create(index_id,index_name,table_id,key_map);
  index_meta_->SerializeTo(meta_page->GetData());
  index_info->Init(index_meta_,table_info,buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(meta_page_id,true);

  dberr_t result = DB_SUCCESS;
  for (auto iter = table_info->GetTableHeap()->Begin(txn);iter != table_info->GetTableHeap()->End()&&result == DB_SUCCESS; ++iter) {
    Row row;
    iter->GetKeyFromRow(table_info->GetSchema(), index_info->GetIndexKeySchema(),row);
    result = index_info->GetIndex()->InsertEntry(row, iter->GetRowId(), txn);
  }
  if(result != DB_SUCCESS){
    index_info->GetIndex()->Destroy();
    buffer_pool_manager_->DeletePage(meta_page_id);
    delete index_info;
    return result;
  }

  index_names_[table_name][index_name]=index_id;
  indexes_[index_id]=index_info;
  catalog_meta_->index_meta_pages_[index_id]=meta_page_id;
  Page* page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char* temp2=page->GetData();
  catalog_meta_->SerializeTo(temp2);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  if(index_names_.find(table_name)==index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, index_id_t> index=index_names_.find(table_name)->second;
  if(index.find(index_name)==index.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id=index[index_name];
  index_info=indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  if(index_names_.find(table_name)==index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, index_id_t> index=index_names_.find(table_name)->second;
  for(auto i:index){
    indexes.push_back(indexes_.find(i.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto temp=table_names_.find(table_name);
  if(temp==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id=table_names_[table_name];
  TableInfo* table_info=tables_[table_id];
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
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto temp=index_names_.find(table_name);
  if(temp==index_names_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  auto temp1=temp->second.find(index_name);
  if(temp1==temp->second.end()){
    return DB_INDEX_NOT_FOUND;
  }
  IndexInfo *index_info = indexes_[temp1->second];
  index_info->GetIndex()->Destroy();
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.at(temp1->second));
  catalog_meta_->index_meta_pages_.erase(temp1->second);
  delete indexes_[temp1->second];
  indexes_.erase(temp1->second);
  temp->second.erase(index_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  Page* page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if(page==nullptr){
    return DB_FAILED;
  }
  catalog_meta_->SerializeTo(page->GetData());
  if (buffer_pool_manager_->FlushPage(page->GetPageId())) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return DB_FAILED;
  } else {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  TableInfo* table_info=nullptr;
  table_info = table_info->Create();
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  TableMetadata* table_meta_=nullptr;
  table_meta_->DeserializeFrom(page->GetData(),table_meta_);
  string table_name=table_meta_->GetTableName();
  page_id_t table_page_id_=table_meta_->GetFirstPageId();
  TableSchema* schema=table_meta_->GetSchema();
  TableHeap* table_heap_=table_heap_->Create(buffer_pool_manager_,table_page_id_,schema,nullptr,nullptr);
  table_info->Init(table_meta_,table_heap_);
  tables_[table_id]=table_info;
  table_names_[table_name]=table_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  IndexInfo* index_info=nullptr;
  index_info=index_info->Create();
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata* index_meta_=nullptr;
  index_meta_->DeserializeFrom(page->GetData(),index_meta_);
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
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto temp=tables_.find(table_id);
  if(temp==tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info=temp->second;
  return DB_SUCCESS;
}
