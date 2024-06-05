#include "catalog/table.h"

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
uint32_t TableMetadata::SerializeTo(char *buf) const {
  // table_id_t table_id_;
  // std::string table_name_;
  // page_id_t root_page_id_;
  // Schema *schema_;
  char *p = buf;
  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize table info.");
  // magic num
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  buf += sizeof(uint32_t);
  // table id
  MACH_WRITE_TO(table_id_t, buf, table_id_);
  buf += sizeof(uint32_t);
  // table name,不定长，需要先记录长度，再记录table_name
  MACH_WRITE_UINT32(buf, table_name_.length());
  buf += 4;
  MACH_WRITE_STRING(buf, table_name_);
  buf += table_name_.length();
  // table heap root page id
  MACH_WRITE_TO(page_id_t, buf, root_page_id_);
  buf += sizeof(page_id_t);
  // table schema
  // 之前实现过，执行schema的serializeTo即可
  buf += schema_->SerializeTo(buf);
  ASSERT(buf - p == ofs, "Unexpected serialize size.");
  return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t TableMetadata::GetSerializedSize() const {
  // table_id_t table_id_;
  // std::string table_name_;
  // page_id_t root_page_id_;
  // Schema *schema_;
  // #define MACH_STR_SERIALIZED_SIZE(Str) (4 + Str.length())
  return (sizeof(uint32_t)+sizeof(table_id_t)+MACH_STR_SERIALIZED_SIZE(table_name_)+sizeof(page_id_t)+schema_->GetSerializedSize());
}

/**
 *
 * @param heap Memory heap passed by TableInfo
 */
// 将buf读取到table_metadata
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta) {
  if (table_meta != nullptr) {
    LOG(WARNING) << "Pointer object table info is not null in table info deserialize." << std::endl;
  }
  char *p = buf;
  // magic num
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
  // table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // table name
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string table_name(buf, len);
  buf += len;
  // table heap root page id
  page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
  buf += 4;
  // table schema
  TableSchema *schema = nullptr;
  buf += TableSchema::DeserializeFrom(buf, schema);
  // allocate space for table metadata
  table_meta = new TableMetadata(table_id, table_name, root_page_id, schema);
  return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema) {
  // allocate space for table metadata
  return new TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
