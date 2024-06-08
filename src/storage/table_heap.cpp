#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

/**
 * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
 * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
 * @param[in] txn The recovery performing the insert
 * @return true iff the insert is successful
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  //If the tuple is too large (>= page_size), return false.
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size >= PAGE_SIZE) {
    return false;
  }
  // Find the page which contains the tuple.
  for(page_id_t id = GetFirstPageId(); id != INVALID_PAGE_ID; ) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(id));
    // 给当前的page上锁
    page->WLatch();
    // 如果该page能够插入tuple
    if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
    // 如果该page不能插入该tuple
    page->WUnlatch();
    // dirty bit = false 表示没有发生修改
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // 检查下一个page
    page_id_t prev_page_id = id;
    id = page->GetNextPageId();
    if(id == INVALID_PAGE_ID) {
      // 此时需要新建一个page
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      // 对当前新建的page和旧page进行上锁
      new_page->WLatch();
      page->WLatch();
      // 初始化newpage
      new_page->Init(new_page_id, prev_page_id, log_manager_, txn);
      // 利用nextpageid进行判断，需将最后一个page的nextpage的id设置为INVAILD_PAGE_ID
      new_page->SetNextPageId(INVALID_PAGE_ID);
      page->SetNextPageId(new_page_id);
      page->WUnlatch();
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), true);
      id = new_page->GetPageId();
    }
  }
  return false;
}
/**
 * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
 * @param[in] rid Resource id of the tuple of delete
 * @param[in] txn Txn performing the delete
 * @return true iff the delete is successful (i.e the tuple exists)
 */
bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
/**
 * if the new tuple is too large to fit in the old page, return false (will delete and insert)
 * @param[in] row Tuple of new row
 * @param[in] rid Rid of the old tuple
 * @param[in] txn Txn performing the update
 * @return true is update is successful.
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  // if the new tuple is too large to fit in the old page, return false (will delete and insert)
  if(row.GetSerializedSize(schema_) > PAGE_SIZE) {
    return false;
  }
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  old_page->WLatch();
  Row old_row = Row(rid);
  //  old_page->GetTuple(&old_row,schema_,txn,lock_manager_);只要有rid就行
  bool update_result = old_page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
  //要求old_row的field是空的
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  return update_result;
}

/**
 * TODO: Student Implement
 */
/**
 * Called on Commit/Abort to actually delete a tuple or rollback an insert.
 * @param rid Rid of the tuple to delete
 * @param txn Txn performing the delete.
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr) {return;}
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  // Step2: Delete the tuple from the page.
}

/**
 * Called on abort to rollback a delete.
 * @param[in] rid Rid of the deleted tuple.
 * @param[in] txn Txn performing the rollback
 */
void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
/**
 * Read a tuple from the table.
 * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
 * @param[in] txn recovery performing the read
 * @return true if the read was successful (i.e. the tuple exists)
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page == nullptr) {return false;}
  page->RLatch();
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  page->RLatch();
  page->GetFirstTupleRid(&rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return TableIterator(this, rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this, INVALID_ROWID, nullptr);
}
