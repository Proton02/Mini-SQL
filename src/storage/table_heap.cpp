#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size > PAGE_SIZE) {
    return false;
  }
  page_id_t page_id = first_page_id_;
  page_id_t prev_page_id = INVALID_PAGE_ID;
  // TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  bool is_sucess = false;
  while (!is_sucess) {
    if (page_id == INVALID_PAGE_ID) {
      buffer_pool_manager_->NewPage(page_id);
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      page->WLatch();
      page->Init(page_id, prev_page_id, log_manager_, txn);
      is_sucess = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
      if (prev_page_id == INVALID_PAGE_ID) {
        first_page_id_ = page_id;
      } else {
        auto temp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
        temp_page->WLatch();
        temp_page->SetNextPageId(page_id);
        temp_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page_id, true);
      }
      break;
    } else {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      page->WLatch();
      is_sucess = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      prev_page_id = page_id;
      page->RLatch();
      page_id = page->GetNextPageId();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    }
  }
  return is_sucess;
}

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
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) {
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  old_page->WLatch();
  Row old_row = Row(rid);
  bool update_result = old_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  return update_result;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

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
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  RowId rid = row->GetRowId();
  if (rid == INVALID_ROWID) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->RLatch();
  bool get_result = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return get_result;
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
