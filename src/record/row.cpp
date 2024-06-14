#include "record/row.h"


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
/**
 * TODO: Student Implement
 */
// 与元组的概念等价，用于存储记录或索引键，一个Row由一个或多个Field构成。
/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // 1. row由一个或多个Field构成，每个Field对应一个Column，Field的序列化格式为Column的序列化格式
  // 先计算field的数量，再计算null bitmap的大小（要求向上取字节）
  uint32_t field_nums = schema->GetColumnCount();
  uint32_t null_bitmap_size = (field_nums + 7) / 8;
  // 使用size记录序列化过程中指针buf向后移动的字节数
  uint32_t size = 0;
  // 2. 先计算header的序列化格式
  // 2.1. 先序列化field的数量
  MACH_WRITE_TO(uint32_t, buf + size, field_nums);
  size += sizeof(field_nums);
  // 2.2. 再序列化null bitmap
  // 初始化null bitmap，将所有位都设为0，表示所有field都不为空
  // 检查fields_[i]是否为空，如果为空，将null bitmap的第i位设为1
  char temp = 0;
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < null_bitmap_size; i++) {
    for (int j = 0; j < 8; j++) {
      char mask = 0;
      temp <<= 1;
      if (cnt < field_nums) {
        mask = fields_[cnt]->IsNull() ? 1 : 0;
      }
      temp |= mask;
      cnt++;
    }
    MACH_WRITE_TO(char, buf + size, temp);
    size += sizeof(char);
    temp = 0;
  }
  // 3. 再序列化field的序列化格式
  for (uint32_t i = 0; i < field_nums; i++) {
    if (!fields_[i]->IsNull()) {
      size += fields_[i]->SerializeTo(buf + size);
    }
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t schema_nums = schema->GetColumnCount();
  uint32_t size = 0;
  // 1. 先反序列化header
  // 1.1. 先反序列化field的数量
  uint32_t field_nums = MACH_READ_FROM(uint32_t, buf + size);
  size += sizeof(uint32_t);
  ASSERT(field_nums == schema_nums, "The total num of buf does not match");
  // 1.2. 再反序列化null bitmap
  std::vector<bool> isnull(field_nums);
  char mask = 1 << 7;
  char temp;
  for (uint32_t i = 0; i < field_nums; i++) {
    if (i % 8 == 0) {
      temp = MACH_READ_FROM(char, buf + size);
      size += sizeof(char);
    }
    isnull[i] = mask & temp;
    temp <<= 1;
  }
  fields_.resize(field_nums);
  for (uint32_t i = 0; i < field_nums; i++) {
    size += fields_[i]->DeserializeFrom(buf + size, schema->GetColumn(i)->GetType(), &fields_[i], isnull[i]);
  }
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  // 1. header的size包含field_num + null_bitmap_size
  uint32_t size = 0;
  uint32_t field_nums = fields_.size();
  size += sizeof(uint32_t);
  size += (field_nums + 7) / 8;
  // 2. 再计算field的size
  for(int i = 0;i < field_nums;i++)
  {
    size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
