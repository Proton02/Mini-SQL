#include "record/row.h"

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
  // replace with your code here
  // 1. row由一个或多个Field构成，每个Field对应一个Column，Field的序列化格式为Column的序列化格式
  // 先计算field的数量，再计算null bitmap的大小（要求向上取字节）
  uint32_t field_num = schema->GetColumnCount();
  uint32_t null_bitmap_size = (field_num + 7) / 8;
  // 使用size记录序列化过程中指针buf向后移动的字节数
  uint32_t size = 0;
  // 2. 先计算header的序列化格式
  // 2.1. 先序列化field的数量
  memcpy(buf, &field_num, sizeof(uint32_t));
  size += sizeof(uint32_t);
  if(field_num == 0) {
    return size;
  }
  // 2.2. 再序列化null bitmap
  char *null_bitmap = buf + size;
  // 初始化null bitmap，将所有位都设为0，表示所有field都不为空
  memset(null_bitmap, 0, null_bitmap_size);
  size += null_bitmap_size;
  // 3. 再计算field的序列化格式
  for(uint32_t i = 0; i < field_num; i++) {
    // 3.1. 先序列化null bitmap
    // 检查fields_[i]是否为空，如果为空，将null bitmap的第i位设为1
    if(fields_[i] == nullptr) {
      null_bitmap[i/8] = null_bitmap[i/8] | (1 << (7 - (i % 8)));
    }
    else {
      // inline uint32_t SerializeTo(char *buf) const { return Type::GetInstance(type_id_)->SerializeTo(*this, buf); }
      size += fields_[i]->SerializeTo(buf + size);
    }
  }
  // delete []null_bitmap;
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t size = 0;
  // 1. 先反序列化header
  uint32_t field_num = 0;
  // 1.1. 先反序列化field的数量
  memcpy(&field_num, buf+size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  if(field_num == 0) {
    return size;
  }
  // 1.2. 再反序列化null bitmap
  uint32_t null_bitmap_size = (field_num + 7) / 8;
  char *null_bitmap = new char[null_bitmap_size];
  // 读取buf中的bull_bitmap
  memcpy(null_bitmap, buf+size, null_bitmap_size * sizeof(char));
  size += null_bitmap_size * sizeof(char);
  // 2. 再反序列化field的序列化格式
  for(uint32_t i = 0; i < field_num; i++) {
    // 2.1 首先得知道读取的field的类型是什么
    TypeId type = schema->GetColumn(i)->GetType();
    // 根据null_bitmap判断当前的field_[i]是否为空
    // 1表示为空，0表示不为空
    if((null_bitmap[i/8] & (1 << (7- (i % 8)))) == 0) {
      // explicit Field(TypeId type, int32_t i) : type_id_(type) {
      //   ASSERT(type == TypeId::kTypeInt, "Invalid type.");
      //   value_.integer_ = i;
      //   len_ = Type::GetTypeSize(type);
      // }
      if(type == TypeId::kTypeInt) {
        int32_t row_value = 0;
        // 从buf+size中读取row_value，然后构建新的field
        memcpy(&row_value, buf+size, sizeof(int32_t));
        size += sizeof(int32_t);
        fields_.emplace_back(new Field(type, row_value));
      }
      // explicit Field(TypeId type, float f) : type_id_(type) {
      //   ASSERT(type == TypeId::kTypeFloat, "Invalid type.");
      //   value_.float_ = f;
      //   len_ = Type::GetTypeSize(type);
      // }
      else if(type == TypeId::kTypeFloat) {
        float row_value = 0;
        memcpy(&row_value, buf+size, sizeof(float));
        size += sizeof(float);
        fields_.emplace_back(new Field(type, row_value));
      }
      // explicit Field(TypeId type, char *data, uint32_t len, bool manage_data)
      // char类型需要4个参数，
      else {
        // 不定长字符串，先读取长度，再读取数据
        uint32_t len = 0;
        memcpy(&len, buf+size, sizeof(uint32_t));
        size += sizeof(uint32_t);
        // 读取字符串的内容
        char *value = new char[len];
        memcpy(value, buf+size, len);
        size += len;
        fields_.emplace_back(new Field(type, value, len, true));
        delete[] value;
      }
    }
    else {
      fields_.emplace_back(nullptr);
    }
  }
  delete[] null_bitmap;
  return size;
<<<<<<< HEAD

=======
>>>>>>> dbf6a3607a31ed04099eef56eb91a0295d8766e9
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  // 1. header的size包含field_num + null_bitmap_size
  uint32_t size = 0;
  size += sizeof(uint32_t);
  uint32_t field_num = schema->GetColumnCount();
  size += (field_num + 7) / 8;
  // 2. 再计算field的size
  for(uint32_t i = 0; i < field_num; i++) {
    if(fields_[i] != nullptr) {
      size += fields_[i]->GetSerializedSize();
    }
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