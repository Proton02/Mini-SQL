#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  // static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;
  // std::string name_;
  // TypeId type_;
  // uint32_t len_{0};  // for char type this is the maximum byte length of the string data,
  // // otherwise is the fixed size
  // uint32_t table_ind_{0};  // column position in table
  // bool nullable_{false};   // whether the column can be null
  // bool unique_{false};     // whether the column is unique

  uint32_t size = 0;
  memcpy(buf + size, &COLUMN_MAGIC_NUM, sizeof(COLUMN_MAGIC_NUM));
  size += sizeof(COLUMN_MAGIC_NUM);
  // name 是不定长的
  uint32_t name_len;
  name_len = name_.length();
  memcpy(buf + size, &name_len, sizeof(name_len));
  size += sizeof(name_len);
  memcpy(buf + size, name_.c_str(), name_len);
  size += name_len;
  memcpy(buf + size, &type_, sizeof(type_));
  size += sizeof(type_);
  memcpy(buf + size, &len_, sizeof(len_));
  size += sizeof(len_);
  memcpy(buf + size, &table_ind_, sizeof(table_ind_));
  size += sizeof(table_ind_);
  memcpy(buf + size, &nullable_, sizeof(nullable_));
  size += sizeof(nullable_);
  memcpy(buf + size, &unique_, sizeof(unique_));
  size += sizeof(unique_);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(COLUMN_MAGIC_NUM)+sizeof(uint32_t)+name_.length()+sizeof(type_)+sizeof(len_)+sizeof(table_ind_)+sizeof(nullable_)+sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t COLUMN_MAGIC_NUM_temp;
  uint32_t size = 0;
  memcpy(&COLUMN_MAGIC_NUM_temp, buf + size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  if (COLUMN_MAGIC_NUM_temp!= COLUMN_MAGIC_NUM) {
    return 0;
  }
  std::string name_temp;
  uint32_t name_length;
  memcpy(&name_length, buf + size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  name_temp.resize(name_length);
  memcpy(&name_temp[0], buf + size, name_length);
  size += name_length;
  TypeId type_temp;
  memcpy(&type_temp, buf + size, sizeof(type_temp));
  size += sizeof(type_temp);
  uint32_t len_temp;
  memcpy(&len_temp, buf + size, sizeof(len_temp));
  size += sizeof(len_temp);
  uint32_t table_ind_temp;
  memcpy(&table_ind_temp, buf + size, sizeof(table_ind_temp));
  size += sizeof(table_ind_temp);
  bool nullable_temp;
  memcpy(&nullable_temp, buf + size, sizeof(nullable_temp));
  size += sizeof(nullable_temp);
  bool unique_temp;
  memcpy(&unique_temp, buf + size, sizeof(unique_temp));
  size += sizeof(unique_temp);
  if(type_temp == TypeId::kTypeChar) {
    column = new Column(name_temp, type_temp, len_temp, table_ind_temp, nullable_temp, unique_temp);
  }
  else
    column = new Column(name_temp, type_temp, table_ind_temp, nullable_temp, unique_temp);
  return size;
<<<<<<< HEAD

}
=======
}
>>>>>>> dbf6a3607a31ed04099eef56eb91a0295d8766e9
