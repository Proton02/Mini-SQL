#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM, int leaf_max_size, int internal_max_size)
          : index_id_(index_id),
            buffer_pool_manager_(buffer_pool_manager),
            processor_(KM), // KeyManager，用于序列化和反序列化
            leaf_max_size_(leaf_max_size),
            internal_max_size_(internal_max_size) {
  // 最大分叉数 = (页大小 - 指针) / (数据大小 + 指针大小)
  if(leaf_max_size == 0){
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(RowId) + processor_.GetKeySize());
  }
  // 内部节点使用page_id_t
  if(internal_max_size == 0){
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(page_id_t) + processor_.GetKeySize()); 
  }
  // 获取页面
  Page* page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* root_page = reinterpret_cast<IndexRootsPage*>(page);
  root_page->GetRootId(index_id,&root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

// 删除当前页
void BPlusTree::Destroy(page_id_t bpt_pageent_page_id) {
  buffer_pool_manager_->DeletePage(bpt_pageent_page_id);
}

/*
 * Helper   function to decide whether bpt_pageent b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  // 根据页id判断即可
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) { 
  auto *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key));
  RowId val;
  if(root_page_id_ == INVALID_PAGE_ID){
    return false;
  }else{
    if(leaf_page->Lookup(key, val, processor_)){ // 找到
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      result.push_back(val);
      return true;
    }else{
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  } 
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if bpt_pageent tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }else{
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 申请新页
  page_id_t new_id;
  Page *test_page = buffer_pool_manager_->NewPage(new_id);
  // 判断内存是否足够
  if(test_page == nullptr)
    throw("Error: get new page failed!");
  // 可以创建新ye
  BPlusTreeLeafPage *new_page = reinterpret_cast<BPlusTreeLeafPage *>(test_page->GetData());
  root_page_id_ = new_id;
  UpdateRootPageId(1);
  new_page->Init(new_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  new_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(new_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
// bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
//   // 找到右边叶节点 false
//   RowId tmp;
//   BPlusTreeLeafPage *insert_tar = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false));
//   bool is_exist = insert_tar->Lookup(key, tmp, processor_);
//   int size = insert_tar->Insert(key, value, processor_);
//   if(is_exist){ // 已经存在
//     buffer_pool_manager_->UnpinPage(insert_tar->GetPageId(), false);
//     return false;
//   }else{  // 不存在，需要插入
//     if(size > leaf_max_size_){
//       BPlusTreeLeafPage *new_page = Split(insert_tar, transaction);
//       InsertIntoParent(insert_tar, new_page->KeyAt(0), new_page, transaction);
//     }else{
//       buffer_pool_manager_->UnpinPage(insert_tar->GetPageId(), true);
//     }
//     return true;
//   }
// }

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  page_id_t id;
  BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(id));
  if(new_page == nullptr){
    throw("Error: out of memory!");
    return nullptr;
  }else{
    new_page->Init(id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_page, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(id, true);
    return new_page;
  }
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  page_id_t id;
  BPlusTreeLeafPage *new_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(id));
  if(new_page == nullptr){
    throw("Error: out of memory!");
    return nullptr;
  }else{
    new_page->Init(id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_page);
    node->SetNextPageId(new_page->GetPageId());
    new_page->SetNextPageId(node->GetNextPageId());
    buffer_pool_manager_->UnpinPage(id, true);
    return new_page;
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  page_id_t id;
  if(old_node->IsRootPage()){ // 创建新的根节点
    auto new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(id)->GetData());
    new_page->Init(id, INVALID_PAGE_ID, old_node->GetKeySize(), leaf_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = id;
    UpdateRootPageId(0);  // 取0，对根进行更新
    old_node->SetParentPageId(id);
    new_node->SetParentPageId(id);
    buffer_pool_manager_->UnpinPage(id, true);
  }else{  // 非根节点
    id = old_node->GetParentPageId();
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    int size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // 超出内部节点大小，需要分裂，递归向上
    if(size >= internal_max_size_){
      BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(Split(parent_page, transaction));
      InsertIntoParent(parent_page, new_page->KeyAt(0), new_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If bpt_pageent tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()){
    return;
  }else{
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false)->GetData());
    // 删除键值对后有两种情况：删除后比半满小，需要合并；
    // 删除了叶子节点第一个元素，需要更新父节点
    // 但实际上这个操作是多余的，中间节点只是起到一个索引作用，所以不需要实现（上课也讲过）
    int new_size = leaf_page->RemoveAndDeleteRecord(key, processor_);
    if(new_size < leaf_page->GetMinSize()){
      bool delete_this_page_flag;
      delete_this_page_flag = CoalesceOrRedistribute(leaf_page, transaction);
      if(delete_this_page_flag){
        buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
      }
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  // case0: node 是根节点，使用 AdjustRoot
  // 从父节点爬到兄弟节点
  // case1：node 不是第一个节点，前一个兄弟大小比最小大小大，调用redis
  // case2：node 无法redis，且不是第一个节点，和前一个合并，调用coalesce
  // case3：node 不是最后一个节点，后一个兄弟大小比最小大小大，调用redis
  // case4：node 无法redis，且是一个节点，和后一个合并，调用coalesce
  // 最后判断父节点是否比最小大小小，如果小就递归
  // true 则删除 node，false 则不删除

  if (node->IsRootPage()) return AdjustRoot(node);
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  N *pre_page, *next_page;
  int node_index = parent_page->ValueIndex(node->GetPageId());

  if(node_index > 0){   
    pre_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index - 1))->GetData());
    if(pre_page->GetSize() > pre_page->GetMinSize()){   // case 1 可以redis，从相邻的兄弟节点中拿来一个元素
      Redistribute(pre_page, node, 1);
      if (!node->IsLeafPage()) {
        InternalPage *tmp;
        Page *now_page = buffer_pool_manager_->FetchPage(node->GetPageId());
        BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());
        while (!bpt_page->IsLeafPage()) {                                   // 向下循环找到叶子节点
          buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(), false);    // 关闭上一层
          tmp = reinterpret_cast<::InternalPage *>(now_page);
          now_page = buffer_pool_manager_->FetchPage(tmp->ValueAt(0));
          bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());
        }
        buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(),false); // 关闭叶子

        BPlusTreeLeafPage * leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(tmp->ValueAt(0)));
        parent_page->SetKeyAt(node_index,leaf_page->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
      }
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->ValueAt(node_index - 1), true);
      return false;
    }else{                                              // case 2
      Coalesce(pre_page, node, parent_page, node_index, transaction);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
      return true;
    }
  }
  if (node_index < parent_page->GetSize() - 1) {  
    next_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(node_index + 1))->GetData());
    if (next_page->GetSize() > next_page->GetMinSize()) {   // case 3
      Redistribute(next_page, node, 0);
      if (!next_page->IsLeafPage()) {
        InternalPage *tmp;
        Page *now_page = buffer_pool_manager_->FetchPage(next_page->GetPageId());
        BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());
        while (!bpt_page->IsLeafPage()) {                                   // 向下循环找到叶子节点
          buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(), false);    // 关闭上一层
          tmp = reinterpret_cast<::InternalPage *>(now_page);
          now_page = buffer_pool_manager_->FetchPage(tmp->ValueAt(0));
          bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());
        }
        buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(),false); // 关闭叶子

        BPlusTreeLeafPage * leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(tmp->ValueAt(0)));
        parent_page->SetKeyAt(node_index,leaf_page->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
      }
    }else{                                              // case 4
      Coalesce(node, next_page, parent_page, node_index + 1, transaction);
    }
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->ValueAt(node_index + 1), true);
    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int idx_tmp = index == 0 ? 1 : index;
  node->MoveAllTo(neighbor_node);
  parent->Remove(idx_tmp);  // 调整父节点
  if(parent->GetSize() < parent->GetMinSize()){
    return CoalesceOrRedistribute(parent, transaction);
  }else{
    return false;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // 通知buffer
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int idx_tmp = index == 0 ? 1 : index;
  node->MoveAllTo(neighbor_node, parent->KeyAt(idx_tmp), buffer_pool_manager_);
  parent->Remove(idx_tmp);
  if(parent->GetSize() < parent->GetMinSize()){
    return CoalesceOrRedistribute(parent, transaction);
  }else{
    return false;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // 通知buffer
}
/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  BPlusTreeInternalPage *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  if(index == 0){ // first
    node->MoveFirstToEndOf(neighbor_node);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  }else{  // last
    neighbor_node->MoveLastToFrontOf(node);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  BPlusTreeInternalPage *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  if(index == 0){ // first
    node->MoveFirstToEndOf(node, parent_node->KeyAt(0), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  }else{  // last
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(1), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetSize() == 1){  // 还有一个
    root_page_id_ = reinterpret_cast<BPlusTreeInternalPage *>(old_root_node)->ValueAt(0);
    UpdateRootPageId(0);
    BPlusTreeInternalPage *new_root = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    new_root->SetParentPageId(INVALID_PAGE_ID); // 设为新根
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }else if(old_root_node->GetSize() == 0){  // 最后一个
    // 直接删掉
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }else{
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  // true 最左边
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, INVALID_PAGE_ID, true));
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false));
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, leaf_page->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  // 先找到最后一个
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, INVALID_PAGE_ID, false));
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, leaf_page->GetSize() - 1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(IsEmpty()) 
    return nullptr;
  // 抓取根节点，从根节点一直到叶子
  Page *now_page = buffer_pool_manager_->FetchPage(root_page_id_);
  page_id_t now_id = root_page_id_;
  // 转成B+树页
  BPlusTreePage *now_bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());

  while(!now_bpt_page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(now_id, false);
    auto *in_bpt_page = reinterpret_cast<InternalPage *>(now_page);
    if(leftMost){ // 最左边
      now_id = in_bpt_page->ValueAt(0);
    }else{  // 一般情况
      now_id = in_bpt_page->Lookup(key, processor_);
    }
    now_page = buffer_pool_manager_->FetchPage(now_id);
    now_bpt_page = reinterpret_cast<BPlusTreePage *>(now_page->GetData());
  }
  return now_page;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * 每次改变根的id的时候都要调用
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, bpt_pageent_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record == 0){ // false 更新
    root_page->Update(index_id_, root_page_id_);
  }else{
    root_page->Insert(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}