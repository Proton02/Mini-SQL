#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
  // std::vector<Column *> columns_;
  // bool is_manage_ = false; /** if false, don't need to delete pointer to column */
  uint32_t size = 0;
  uint32_t column_num = columns_.size();
  memcpy(buf + size, &column_num, sizeof(uint32_t));
  size += sizeof(uint32_t);
  for(auto &column : columns_) {
    size += column->SerializeTo(buf + size);
  }
  memcpy(buf + size, &is_manage_, sizeof(bool));
  size += sizeof(bool);
  return size;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = 0;
  size += sizeof(uint32_t);
  for(auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  size += sizeof(is_manage_);
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
  }
  uint32_t size = 0;
  uint32_t column_num = MACH_READ_FROM(uint32_t, buf + size);
  size += sizeof(uint32_t);
  std::vector<Column *> columns(column_num);
  for (uint32_t i = 0; i < column_num; i++) {
    size += Column::DeserializeFrom(buf + size, columns[i]);
  }
  bool is_manage;
  is_manage = MACH_READ_FROM(bool, buf + size);
  size += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return size;
}