#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  // static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
  // std::vector<Column *> columns_;
  // bool is_manage_ = false; /** if false, don't need to delete pointer to column */
  uint32_t size = 0;
  memcpy(buf + size, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  size += sizeof(uint32_t);
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
  size += sizeof(SCHEMA_MAGIC_NUM);
  size += sizeof(uint32_t);
  for(auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  size += sizeof(is_manage_);
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t SCHEMA_MAGIC_NUM_temp;
  uint32_t column_num_temp;
  uint32_t size = 0;
  bool is_manage_temp;
  memcpy(&SCHEMA_MAGIC_NUM_temp, buf + size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  if(SCHEMA_MAGIC_NUM_temp!= SCHEMA_MAGIC_NUM) {
    return 0;
  }
  memcpy(&column_num_temp, buf + size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  std::vector<Column *>columns_temp;
  for(uint32_t i=0; i < column_num_temp; i++) {
    Column *temp;
    size += Column::DeserializeFrom(buf + size, temp);
    columns_temp.push_back(temp);
  }
  memcpy(&is_manage_temp, buf + size, sizeof(bool));
  size += sizeof(bool);
  schema = new Schema(columns_temp, is_manage_temp);
  return size;
}