#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */

bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }

  LockPrepare(txn, rid);
  LockRequestQueue &request = lock_table_[rid];
  request.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
  if (request.is_writing_) {
    auto condition = [&request, txn]() -> bool {
      bool result= txn->GetState() == TxnState::kAborted || !request.is_writing_;
      return result;
    };
    while (!condition()) {
      request.cv_.wait(lock);
    }  
  }
  CheckAbort(txn, request);
  txn->GetSharedLockSet().emplace(rid);
  request.sharing_cnt_++;
  auto iter = request.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kShared;
  return true;
}

/**
 * TODO: Student Implement
 */

bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockPrepare(txn, rid);
  LockRequestQueue &request = lock_table_[rid];

  request.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);

  if (request.is_writing_ || request.sharing_cnt_ > 0) {
    auto condition=[&request, txn]()->bool{
        auto result= txn->GetState() == TxnState::kAborted || (!request.is_writing_ && !request.sharing_cnt_);
        return result;
    };
    while(!condition()){
      request.cv_.wait(lock);
    }
  }

  CheckAbort(txn, request);
  txn->GetExclusiveLockSet().emplace(rid);
  request.is_writing_ = true;
  auto iter = request.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kExclusive;
  return true;
}

/**
 * TODO: Student Implement
 */

bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }

  LockRequestQueue &request = lock_table_[rid];
  if (request.is_upgrading_) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }
  auto iter = request.GetLockRequestIter(txn->GetTxnId());
  if (LockMode::kExclusive == iter->lock_mode_ && LockMode::kExclusive == iter->granted_) {
    return true;
  }
  iter->lock_mode_ = LockMode::kExclusive;
  iter->granted_ = LockMode::kShared;

  if (request.is_writing_ || request.sharing_cnt_ > 1) {
    request.is_upgrading_ = true;
    auto condition = [&request, txn]() -> bool {
      auto result = txn->GetState() == TxnState::kAborted || (!request.is_writing_ && 1 == request.sharing_cnt_);
      return result;
    };
    while(!condition()){
      request.cv_.wait(lock);
    }
  }
  if (txn->GetState() == TxnState::kAborted) {
    request.is_upgrading_ = false;
  }
  CheckAbort(txn, request);
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().emplace(rid);
  request.sharing_cnt_--;
  request.is_upgrading_ = false;
  request.is_writing_ = true;
  iter->granted_ = LockMode::kExclusive;
  return true;
}

/**
 * TODO: Student Implement
 */

bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockRequestQueue &request = lock_table_[rid];
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().erase(rid);
  auto iter = request.GetLockRequestIter(txn->GetTxnId());
  auto lock_mode = iter->lock_mode_;
  bool res = request.EraseLockRequest(txn->GetTxnId());
  assert(res);

  if (txn->GetState() == TxnState::kGrowing &&!(txn->GetIsolationLevel() == IsolationLevel::kReadCommitted && LockMode::kShared == lock_mode)) {
    txn->SetState(TxnState::kShrinking);
  }

  if (LockMode::kShared == lock_mode) {
    request.sharing_cnt_--;
    request.cv_.notify_all();
  } else {
    request.is_writing_ = false;
    request.cv_.notify_all();
  }
  return true;
}

/**

 * TODO: Student Implement
 */

void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

/**
 * TODO: Student Implement
 */

void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

/**

 * TODO: Student Implement
 */

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { 
  waits_for_[t1].insert(t2); 
}

/**
 * TODO: Student Implement
 */

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { 
  waits_for_[t1].erase(t2); 
}

/**
 * TODO: Student Implement
 */

bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  revisited_node_ = INVALID_TXN_ID;
  visited_set_.clear();
  std::stack<txn_id_t>().swap(visited_path_);
  std::set<txn_id_t> txn_set;
  for (auto it = waits_for_.begin(); it != waits_for_.end(); it++) { 
    auto temp1 = it->first;
    txn_set.insert(temp1);
    auto vec = it->second;
    for (auto it_vec = vec.begin(); it_vec != vec.end(); it_vec++) {
        auto temp2 = *it_vec; 
        txn_set.insert(temp2);
    }
  }
  for (auto it = txn_set.begin(); it != txn_set.end(); it++) {
    auto start_id = *it;
    if (DFS(start_id)) {
      newest_tid_in_cycle = revisited_node_;
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
          newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
          visited_path_.pop();
      }
      return true;
    }
  }
  newest_tid_in_cycle = INVALID_TXN_ID;
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id : txn->GetSharedLockSet()) {
      for (const auto &lock_req : lock_table_[row_id].req_list_) {
        if (lock_req.granted_ == LockMode::kNone) {
          RemoveEdge(lock_req.txn_id_, txn_id);
        }
      }
    }

    for (const auto &row_id : txn->GetExclusiveLockSet()) {
      for (const auto &lock_req : lock_table_[row_id].req_list_) {
        if (lock_req.granted_ == LockMode::kNone) {
          RemoveEdge(lock_req.txn_id_, txn_id);
        }
      }
    }
}



/**
 * TODO: Student Implement
 */

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval_);
    {
      std::unique_lock<std::mutex> lock(latch_);
      std::unordered_map<txn_id_t, RowId> required_rec;
      for (const auto &[row_id, locked_request_queue] : lock_table_) {
        for (const auto &locked_request : locked_request_queue.req_list_) {
          if (locked_request.granted_ != LockMode::kNone) continue;
          required_rec[locked_request.txn_id_] = row_id;
          for (const auto &granted_req : locked_request_queue.req_list_) {
            if (LockMode::kNone == granted_req.granted_) continue;
            AddEdge(locked_request.txn_id_, granted_req.txn_id_);
          }
        }
      }
      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(txn_id)) {
        auto *temptxn = txn_mgr_->GetTransaction(txn_id);
        DeleteNode(txn_id);
        temptxn->SetState(TxnState::kAborted);
        lock_table_[required_rec[txn_id]].cv_.notify_all();
      }
      waits_for_.clear();
    }
  }
}

bool LockManager::DFS(txn_id_t txn_id) {
  if (visited_set_.find(txn_id) != visited_set_.end()) {
    revisited_node_ = txn_id;
    return true;
  }
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);
  auto wait_for_txn_id_vec = waits_for_[txn_id];
  for (auto it = wait_for_txn_id_vec.begin(); it != wait_for_txn_id_vec.end(); it++) {
    auto wait_for_txn_id = *it;
    if (DFS(wait_for_txn_id)) {
      return true;
    }
  }
  visited_set_.erase(txn_id);
  visited_path_.pop();
  return false;
}

/**
 * TODO: Student Implement
 */

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto it = waits_for_.begin(); it != waits_for_.end(); it++) {
    auto t1 = it->first;
    auto sibling = it->second;
    for (auto iter = sibling.begin(); iter != sibling.end(); iter++) {
      auto t2 = *iter;
      result.push_back(std::make_pair(t1, t2));
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}
