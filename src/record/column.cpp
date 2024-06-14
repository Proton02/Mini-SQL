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

// static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;
// std::string name_;
// TypeId type_;
// uint32_t len_{0};  // for char type this is the maximum byte length of the string data,
// // otherwise is the fixed size
// uint32_t table_ind_{0};  // column position in table
// bool nullable_{false};   // whether the column can be null
// bool unique_{false};     // whether the column is unique
/**
 * TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t size = 0;
  // 1. name_的长度
  uint32_t name_len = name_.size();
  MACH_WRITE_TO(uint32_t, buf + size, name_len);
  size += sizeof(name_len);
  // 2. name_的内容
  MACH_WRITE_STRING(buf + size, name_);
  size += name_len;
  // 3. type_
  MACH_WRITE_TO(TypeId, buf + size, type_);
  size += sizeof(TypeId);
  // 4. len_{0}
  MACH_WRITE_TO(uint32_t, buf + size, len_);
  size += sizeof(uint32_t);
  // 5. table_ind_{0}
  MACH_WRITE_TO(uint32_t, buf + size, table_ind_);
  size += sizeof(uint32_t);
  // 6. nullable_{false}
  MACH_WRITE_TO(bool, buf + size, nullable_);
  size += sizeof(bool);
  // 7. unique_{false}
  MACH_WRITE_TO(bool, buf + size, unique_);
  size += sizeof(bool);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) + name_.size() + sizeof(TypeId) + sizeof(uint32_t) * 2 + sizeof(bool) * 2;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }
  uint32_t size = 0;
  uint32_t name_len;
  std::memcpy(&name_len, buf + size, sizeof(name_len));
  size += sizeof(name_len);

  std::string name;
  name.resize(name_len);
  memcpy(&name[0], buf + size, name_len);
  size += name_len;

  TypeId type;
  type = MACH_READ_FROM(TypeId, buf + size);
  size += sizeof(type);

  uint32_t len;
  len = MACH_READ_FROM(uint32_t, buf + size);
  size += sizeof(len);

  uint32_t table_index;
  table_index = MACH_READ_FROM(uint32_t, buf + size);
  size += sizeof(table_index);

  bool nullable;
  nullable = MACH_READ_FROM(bool, buf + size);
  size += sizeof(nullable);

  bool unique;
  unique = MACH_READ_FROM(bool, buf + size);
  size += sizeof(unique);

  if (type == kTypeChar) {
    column = new Column(name, type, len, table_index, nullable, unique);
  } else {
    column = new Column(name, type, table_index, nullable, unique);
  }

  return size;
}

