#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:

    /**
    * TODO: Student Implement
    
    */

    void Init(CheckPoint &last_checkpoint) {
      active_txns_ = std::move(last_checkpoint.active_txns_);
      data_ = std::move(last_checkpoint.persist_data_);
      persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
      auto i = log_recs_.begin();
      while(i!=log_recs_.end()){
        if(i->first>persist_lsn_)break;
        i++;
      }
      while (i != log_recs_.end()) {
        auto &temprec = *(i->second);
        active_txns_[temprec.txn_id_] = temprec.lsn_;
        switch (temprec.type_) {
          case LogRecType::kInsert:
            data_[temprec.insert_key] = temprec.insert_value;
            break;
          case LogRecType::kDelete:
            data_.erase(temprec.delete_key);
            break;
          case LogRecType::kUpdate:
            data_.erase(temprec.old_key);
            data_[temprec.new_key] = temprec.new_val;
            break;
          case LogRecType::kCommit:
            active_txns_.erase(temprec.txn_id_);
            break;
          case LogRecType::kAbort:
            Rollback(temprec.txn_id_);
            active_txns_.erase(temprec.txn_id_);
            break;
          default: break;
        }
        i++;
      }
    }

    /**
    * TODO: Student Implement
    */

    void UndoPhase() {
      for (auto i = active_txns_.begin(); i != active_txns_.end(); i++) {
        auto txn_id = i->first;
        Rollback(txn_id);
      }
      active_txns_.clear();
    }

    void Rollback(txn_id_t txn_id) {
      auto last_txn = active_txns_[txn_id];
      while (last_txn != INVALID_LSN) {
        auto temprec = log_recs_[last_txn];
        if (temprec == nullptr) break;
        switch (temprec->type_) {
          case LogRecType::kInsert:
            data_.erase(temprec->insert_key);
            break;
          case LogRecType::kDelete:
            data_[temprec->delete_key] = temprec->delete_value;
            break;
          case LogRecType::kUpdate:
            data_.erase(temprec->new_key);
            data_[temprec->old_key] = temprec->old_val;
            break;
          default:break;
        }
        last_txn = temprec->prev_lsn_;
      }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
