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
  // 1. If the tuple is too large (>= page_size), return false.
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size > PAGE_SIZE) {
    return false;
  }
  // 2. Find the page which contains the tuple.
  page_id_t page_id = first_page_id_;
  page_id_t prev_page_id = INVALID_PAGE_ID;
  // TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  bool is_sucess = false;
  while (!is_sucess) {
    // 如果page_id为INVALID_PAGE_ID，说明需要新建一个page
    if (page_id == INVALID_PAGE_ID) {
      // 新建一个page，page_id为新建的page的id
      buffer_pool_manager_->NewPage(page_id);
      // 通过page_id获取page
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      // 对新建的page进行上锁，write lock
      page->WLatch();
      page->Init(page_id, prev_page_id, log_manager_, txn);
      is_sucess = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      // 更新first_page_id_ 和 next_page_id
      // 如果prev_page_id为INVALID_PAGE_ID，说明是第一个page
      if (prev_page_id == INVALID_PAGE_ID) {
        first_page_id_ = page_id;
      } else {
        // 如果不是第一个page，需要更新prev_page_id的next_page_id
        auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
        prev_page->WLatch();
        prev_page->SetNextPageId(page_id);
        prev_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page_id, true);
      }
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
      break;
    }
    // 如果page_id不为INVALID_PAGE_ID，说明page已经存在
    else {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      page->WLatch();
      is_sucess = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      // 更新prev_page_id 和 page_id
      prev_page_id = page_id;
      page_id = page->GetNextPageId();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    }
  }
  return is_sucess;
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
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) {
  // if the new tuple is too large to fit in the old page, return false (will delete and insert)
  if(row.GetSerializedSize(schema_) > PAGE_SIZE) {
    return false;
  }
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  old_page->WLatch();
  Row old_row = Row(rid);
  //  old_page->GetTuple(&old_row,schema_,txn,lock_manager_);只要有rid就行
  bool update_result = old_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
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
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
  RowId rid = row->GetRowId();
  if (rid == INVALID_ROWID) return false;
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
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

bool TableHeap::GetNextTuple(const Row &row, Row &next_row, Txn *txn) {
  RowId rid = row.GetRowId();
  page_id_t page_id = rid.GetPageId();
  bool is_get = false;
  while (!is_get) {
    if (page_id == INVALID_PAGE_ID) return false;
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    ASSERT(page != nullptr, "page is nullptr , page_id is wrong");
    page->RLatch();
    if (page->GetNextTupleRid(rid, &rid)) {
      is_get = true;
      next_row.destroy();
      next_row.SetRowId(rid);
      page->GetTuple(&next_row, schema_, txn, lock_manager_);
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    } else {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page_id = page->GetNextPageId();
    }
  }
  return is_get;
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;
  bool is_get = false;
  RowId rid;
  while (!is_get) {
    if (page_id == INVALID_PAGE_ID) return TableIterator(nullptr, RowId(), nullptr);
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    ASSERT(page != nullptr, "page is nullptr , page_id is wrong");
    page->RLatch();
    if (page->GetFirstTupleRid(&rid)) {
      is_get = true;
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    } else {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page_id = page->GetNextPageId();
    }
  }
  return TableIterator(this, rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
