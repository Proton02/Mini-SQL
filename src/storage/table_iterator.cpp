#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  row_.SetRowId(rid);
  if (table_heap->GetTuple(&row_, txn)) {
    table_heap_ = table_heap;
    txn_ = txn;
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
  txn_ = other.txn_;
}

TableIterator::TableIterator(TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (table_heap_ == nullptr && itr.table_heap_ == nullptr) {
    return true;
  }
  return table_heap_ == itr.table_heap_ && row_.GetRowId() == itr.row_.GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if (table_heap_ == nullptr && itr.table_heap_ == nullptr) {
    return false;
  }
  return !(table_heap_ == itr.table_heap_ && row_.GetRowId() == itr.row_.GetRowId());
}

const Row &TableIterator::operator*() {
  return row_;
}

Row *TableIterator::operator->() {
  return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  row_ = itr.row_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if(table_heap_->GetNextTuple(row_, row_, txn_)) {
    return *this;
  } else {
    table_heap_ = nullptr;
    txn_ = nullptr;
    return *this;
  }
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++*this;
  return temp;
}
