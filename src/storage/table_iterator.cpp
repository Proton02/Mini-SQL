#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  this->table_heap = table_heap;
  this->cur_row = new Row(rid);
  txn = nullptr;
}
TableIterator::TableIterator(const TableIterator &other) {
  this->table_heap = other.table_heap;
  this->cur_row = other.cur_row;
  this->txn = other.txn;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(this->table_heap == itr.table_heap && this->cur_row == itr.cur_row) {
    return true;
  }
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(this->table_heap == itr.table_heap && this->cur_row == itr.cur_row) {
    return false;
  }
  return true;
}

const Row &TableIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  // 该函数返回当前迭代器指向的Row对象
  return *cur_row;
}

Row *TableIterator::operator->() {
  // 该函数返回当前迭代器指向的Row对象的指针
  return cur_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  // 该函数实现迭代器的赋值操作
  // 1. 将itr的table_heap和cur_row_id赋值给当前迭代器
  // 2. 返回当前迭代器
  this->table_heap = itr.table_heap;
  this->cur_row = itr.cur_row;
  this->txn = itr.txn;

  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if(table_heap == nullptr) {
    LOG(ERROR) <<"mistake" <<std::endl;
  }
  auto page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(cur_row->GetRowId().GetPageId()));
  RowId next_rowid;
  if(page->GetNextTupleRid(cur_row->GetRowId(),&next_rowid)){
    cur_row->SetRowId(next_rowid);
    table_heap->GetTuple(cur_row, nullptr);
    table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return *this;
  }
  table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  while(page->GetNextPageId() != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    if(page->GetNextTupleRid(cur_row->GetRowId(),&next_rowid)){
      cur_row->SetRowId(next_rowid);
      table_heap->GetTuple(cur_row, nullptr);
      table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return *this;
    }
    table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  cur_row->SetRowId(RowId(INVALID_PAGE_ID));
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  ++(*this);
  return TableIterator(*this);
}
