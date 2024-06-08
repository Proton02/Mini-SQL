#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetKeySize(key_size);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    // 当前指针数
    SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
// 返回index处指向子节点的指针
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}
// 把index处的key改为新的key值
void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}
// 返回index所指的值
page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}
// 设置index所指的值
void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}
// 查找value的位置，返回index
int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}
// 返回index处的key
void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}
// 复制内部节点的键值对从src到dest
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  if(GetSize() == 0) return -1;
  else if(GetSize() == 1 || KM.CompareKeys(key, KeyAt(1)) < 0) return ValueAt(0);
  else{
    int l = 1, r = GetSize();
    while(l < r - 1){
      int mid = (l + r) / 2;
      int cmp_res = KM.CompareKeys(key, KeyAt(mid));
      if(cmp_res < 0) r = mid;
      else if(cmp_res > 0) l = mid;
      else if(cmp_res == 0) return ValueAt(mid);
    }
    return ValueAt(l);
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);
  // 只需要设置第一个key和前两个value
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int old_idx = ValueIndex(old_value);
  // PairCopy(PairPtrAt(old_idx + 1), PairPtrAt(old_idx), GetSize() - old_idx - 1);
  for(int i = GetSize() - 1; i > old_idx; i--){
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  SetKeyAt(old_idx + 1, new_key);
  SetValueAt(old_idx + 1, new_value);
  SetSize(GetSize() + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * 把当前页的一半数据移动到recipient页
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 * CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager)
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = GetSize() / 2;
  int begin = GetSize() - GetSize() / 2;
  // 把后一半的数据移动到recipient页
  recipient->CopyNFrom(PairPtrAt(begin), half, buffer_pool_manager);
  SetSize(begin);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  // 转成char，按字节读写
  char *source = reinterpret_cast<char *>(src);

  for (int i = 0; i < size; i++) {
    GenericKey *key = reinterpret_cast<GenericKey *>(source);
    source += GetKeySize();
    page_id_t *pageId = reinterpret_cast<page_id_t *>(source);
    source += sizeof(page_id_t);
    // 把key和value加到当前页的末尾
    CopyLastFrom(key, *pageId, buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  // 序号比大小少一
  for(int i = index; i < GetSize() - 1; i++){
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  // 判断当前页是否只有一个指针
  if(GetSize() == 1){
    return ValueAt(0);
    Remove(0);
  } else {
    return -1;
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  // 把middle_key加到recipient页
  // 和拷贝类似的函数都可以用之后实现的
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  // 把当前页的所有数据移动到recipient页
  recipient->CopyNFrom(PairPtrAt(1), GetSize() - 1, buffer_pool_manager);
  buffer_pool_manager->DeletePage(GetPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 把key和value加到当前页的末尾
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  SetSize(GetSize() + 1);
  // 更新page头文件
  // 由于我们的内部节点是从父类继承来的，所以需要强制转换page，变成内部节点的page，用完之后Unipin掉，不然过不了测试
  InternalPage *interpage = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
  interpage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
  recipient->SetKeyAt(1, middle_key);  // 把middle_key加到recipient第一个的后面
  Remove(GetSize() - 1);  // Remove函数中有SetSize的功能
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 把key和value加到当前页的开头
  InsertNodeAfter(-1, KeyAt(0), value);
  // 更新page头文件
  InternalPage *interpage = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
  interpage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}