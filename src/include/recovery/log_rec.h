#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */

struct LogRec {
  LogRec() = default;

  LogRecType type_{LogRecType::kInvalid};

  lsn_t lsn_{INVALID_LSN};

  lsn_t prev_lsn_{INVALID_LSN};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;

  txn_id_t txn_id_{INVALID_TXN_ID};
  LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn): type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

  KeyType insert_key{};
  ValType insert_value{};
  KeyType delete_key{};
  ValType delete_value{};
  KeyType old_key{};
  ValType old_val{};
  KeyType new_key{};
  ValType new_val{};


  static lsn_t update_lsn(txn_id_t txn_id, lsn_t cur_lsn) {
    auto i = prev_lsn_map_.find(txn_id);
    auto prevlsn = INVALID_LSN;
    if (i != prev_lsn_map_.end()) {
      prevlsn = i->second;
      i->second = cur_lsn;
    } else {
      prev_lsn_map_.emplace(txn_id, cur_lsn);
    }
    return prevlsn;
  }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  auto lsn=LogRec::next_lsn_++;
  auto temp=LogRec::update_lsn(txn_id, lsn);
  auto log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, temp);
  log->insert_key = std::move(ins_key);
  log->insert_value = ins_val;
  return log;
}

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  auto lsn = LogRec::next_lsn_++;
  auto temp = LogRec::update_lsn(txn_id, lsn);
  auto log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, temp);
  log->delete_key = std::move(del_key);
  log->delete_value = del_val;
  return log;
}

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  auto lsn = LogRec::next_lsn_++;
  auto temp = LogRec::update_lsn(txn_id, lsn);
  auto log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, temp);
  log->old_key = std::move(old_key);
  log->old_val = old_val;
  log->new_key = std::move(new_key);
  log->new_val = new_val;
  return log;
}

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  auto lsn = LogRec::next_lsn_++;
  auto temp = LogRec::update_lsn(txn_id, lsn);
  auto result = std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, temp);
  return result;
}

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  auto lsn = LogRec::next_lsn_++;
  auto temp = LogRec::update_lsn(txn_id, lsn);
  auto result = std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, temp);
  return result;
}

/**
 * TODO: Student Implement
 */

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  auto lsn = LogRec::next_lsn_++;
  auto temp = LogRec::update_lsn(txn_id, lsn);
  auto result = std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, temp);
  return result;
}

#endif  // MINISQL_LOG_REC_H
