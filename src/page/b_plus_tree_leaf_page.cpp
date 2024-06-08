#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID); // 下一个是空的
  SetMaxSize(max_size);
  SetKeySize(key_size);
}

/**
 * Helper methods to set/get next page id
 */
// 下一页
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}
// 设置下一页
void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  // 叶子节点键的数量和值的数量是相等的
  // 这里不能把r设为GetSize() - 1，会导致最后一个元素无法访问
  int l = 0, r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    int cmp_res = KM.CompareKeys(key, KeyAt(mid));
    if(cmp_res < 0) r = mid;
    else if(cmp_res > 0) l = mid + 1;
    else return mid;
  }
  return l;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
// 返回指针
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}
// 设置键
void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}
// 返回值
RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}
// 设置值
void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}
// 返回键值对
void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}
// 拷贝键值对
void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { 
  return {KeyAt(index), ValueAt(index)}; 
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  // 插入的位置
  int idx = KeyIndex(key, KM);

  if(idx == -1){    // 插入到最后
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    SetSize(GetSize() + 1);
    return GetSize();
  }else if(idx >= GetSize()){ 
    SetKeyAt(idx, key);
    SetValueAt(idx, value);
    SetSize(GetSize() + 1);
    return GetSize();
  }else if(KM.CompareKeys(key, KeyAt(idx)) == 0){  // 已经存在
    return GetSize();
  }
  // 移动后面的元素
  for(int i = GetSize() - 1; i >= idx; i--){
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  // 插入新元素
  SetKeyAt(idx, key);
  SetValueAt(idx, value);
  // 更新大小
  SetSize(GetSize() + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(GetSize() - GetSize() / 2), GetSize() / 2);
  SetSize(GetSize() - GetSize() / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(PairPtrAt(GetSize()), src, size);
  SetSize(size + GetSize());
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int idx = KeyIndex(key, KM);
  if(idx < GetSize() && KM.CompareKeys(key, KeyAt(idx)) == 0){  // 没有溢出并且存在
    value = ValueAt(idx);
    return true;
  }else{
    return false;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  RowId val;
  if(Lookup(key, val, KM)){
    int idx = KeyIndex(key, KM);
    for(int i = idx; i < GetSize() - 1; i++){
      SetKeyAt(i, KeyAt(i + 1));
      SetValueAt(i, ValueAt(i + 1));
    }
    SetSize(GetSize() - 1);
    return GetSize();
  }else{
    return GetSize();
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * 把第一个键值对移动到recipient的最后
 * 然后整体向前移动
 * 
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  PairCopy(PairPtrAt(0), PairPtrAt(1), GetSize() - 1);
  SetSize(GetSize() - 1);
}

/*
 * 把键值对放在最后
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  SetSize(GetSize() + 1);
}

/*
 * 把最后一个键值对放在recipient的最前面
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  SetSize(GetSize() - 1);
}

/*
 * 把键值对放在最前面
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  // 先整体往后移动
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(GetSize() + 1);
}