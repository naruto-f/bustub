//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn == nullptr) {
    return false;
  }

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> request_queue;
  auto txn_id = txn->GetTransactionId();
  std::unique_lock<std::mutex> tables_lock(table_lock_map_latch_);
  if (!run_) {
    return false;
  }

  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid].reset(new LockRequestQueue);
  }

  request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  tables_lock.unlock();

  LockRequest *request{nullptr};
  if (request_queue->locked_requests_.count(txn_id) != 0) {
    request = request_queue->locked_requests_[txn_id];
    if (request->lock_mode_ == lock_mode) {
      return true;
    }
    UpgradeLockTable(txn, request->lock_mode_, lock_mode, request_queue.get(), oid);
    request->lock_mode_ = lock_mode;
    request_queue->upgrading_ = txn_id;
    request_queue->locked_requests_.erase(txn_id);
  }

  LockRequest *new_request{nullptr};
  if (request_queue->upgrading_ == txn_id) {
    new_request = request;
  } else {
    new_request = new LockRequest(txn_id, lock_mode, oid);
  }

  // std::unique_lock<std::mutex> lock(request_queue->latch_);
  // request_queue->request_queue_.push_back(new_request);
  if (request_queue->locked_requests_.empty()) {
    if (request_queue->upgrading_ == txn_id || request_queue->request_queue_.empty()) {
      if (request_queue->upgrading_ == txn_id) {
        request_queue->upgrading_ = INVALID_TXN_ID;
      }
      request_queue->locked_requests_.insert({txn_id, new_request});
      switch (lock_mode) {
        case LockMode::INTENTION_SHARED: {
          auto is_table_set = txn->GetIntentionSharedTableLockSet();
          is_table_set->insert(oid);
        } break;
        case LockMode::SHARED: {
          auto s_table_set = txn->GetSharedTableLockSet();
          s_table_set->insert(oid);
        } break;
        case LockMode::INTENTION_EXCLUSIVE: {
          auto ie_table_set = txn->GetIntentionExclusiveTableLockSet();
          ie_table_set->insert(oid);
        } break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE: {
          auto sie_rows_set = txn->GetSharedIntentionExclusiveTableLockSet();
          sie_rows_set->insert(oid);
        } break;
        case LockMode::EXCLUSIVE: {
          auto e_rows_set = txn->GetExclusiveTableLockSet();
          e_rows_set->insert(oid);
        } break;
        default:
          break;
      }
      return true;
    }
  }

  {
    std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
    wait_for_lock_map_[txn_id] = request_queue;
  }

  if (request_queue->upgrading_ == txn_id) {
    request_queue->request_queue_.push_front(new_request);
  } else {
    request_queue->request_queue_.push_back(new_request);
  }

  TransactionState state;
  request_queue->cv_.wait(lock, [&]() -> bool {
    txn->LockTxn();
    state = txn->GetState();
    if (state == TransactionState::ABORTED) {
      txn->UnlockTxn();
      return true;
    }
    txn->UnlockTxn();

    if (!IsCompatible(request_queue->request_queue_.front()->lock_mode_, request_queue,
                      request_queue->request_queue_.front())) {
      return false;
    }
    //    if (request_queue->upgrading_ != INVALID_TXN_ID && request_queue->upgrading_ != txn_id) {
    //      return false;
    //    }

    return IsCompatible(lock_mode, request_queue, new_request);
  });

  {
    std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
    wait_for_lock_map_.erase(txn_id);
  }

  if (state == TransactionState::ABORTED) {
    auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
    if (iter != request_queue->request_queue_.end()) {
      request_queue->request_queue_.erase(iter);
    }

    if (request_queue->upgrading_ == txn_id) {
      request_queue->upgrading_ = INVALID_TXN_ID;
    }

    // txn_manager_->Abort(txn);
    delete new_request;
    request_queue->cv_.notify_all();
    return false;
  }

  if (request_queue->upgrading_ == txn_id) {
    request_queue->upgrading_ = INVALID_TXN_ID;
    request_queue->request_queue_.pop_front();
  } else {
    auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
    if (iter != request_queue->request_queue_.end()) {
      request_queue->request_queue_.erase(iter);
    }
  }
  request_queue->locked_requests_.insert({txn_id, new_request});
  // request_queue->cv_.notify_all();

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED: {
      auto is_table_set = txn->GetIntentionSharedTableLockSet();
      is_table_set->insert(oid);
    } break;
    case LockMode::SHARED: {
      auto s_table_set = txn->GetSharedTableLockSet();
      s_table_set->insert(oid);
    } break;
    case LockMode::INTENTION_EXCLUSIVE: {
      auto ie_table_set = txn->GetIntentionExclusiveTableLockSet();
      ie_table_set->insert(oid);
    } break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      auto sie_rows_set = txn->GetSharedIntentionExclusiveTableLockSet();
      sie_rows_set->insert(oid);
    } break;
    case LockMode::EXCLUSIVE: {
      auto e_rows_set = txn->GetExclusiveTableLockSet();
      e_rows_set->insert(oid);
    } break;
    default:
      break;
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (txn == nullptr) {
    return false;
  }

  auto txn_id = txn->GetTransactionId();
  std::shared_ptr<LockRequestQueue> request_queue;

  if (txn->GetSharedTableLockSet()->count(oid) == 0 && txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
      txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
      txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0 &&
      txn->GetExclusiveTableLockSet()->count(oid) == 0) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  std::unique_lock<std::mutex> tables_lock(table_lock_map_latch_);
  request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  tables_lock.unlock();

  //  if (request_queue->locked_requests_.count(txn_id) == 0) {
  //    txn->LockTxn();
  //    txn->SetState(TransactionState::ABORTED);
  //    txn->UnlockTxn();
  //    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  //  }

  auto request = request_queue->locked_requests_[txn_id];
  //  txn->LockTxn();
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if ((s_row_lock_set->count(oid) != 0 && !(*s_row_lock_set)[oid].empty()) ||
      (x_row_lock_set->count(oid) != 0 && !(*x_row_lock_set)[oid].empty())) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  txn->LockTxn();
  auto txn_state = txn->GetState();
  auto lock_mode = request->lock_mode_;
  if (txn_state != TransactionState::ABORTED) {
    auto isolation_level = txn->GetIsolationLevel();
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
    } else if (isolation_level == IsolationLevel::READ_COMMITTED ||
               isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }
  txn->UnlockTxn();

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    default:
      break;
  }

  // txn->UnlockTxn();

  request_queue->locked_requests_.erase(txn_id);
  delete request;
  request_queue->cv_.notify_all();
  lock.unlock();

  //  {
  //    std::lock_guard<std::mutex> wlock(waits_for_latch_);
  //    std::vector<std::pair<txn_id_t, txn_id_t>> pairs;
  //    // TODO(naruto) : fault here！
  //    std::for_each(wait_pairs_.begin(), wait_pairs_.end(), [&](const std::pair<txn_id_t, txn_id_t> &pair) {
  //      if (pair.second == txn_id || pair.first == txn_id) {
  //        pairs.push_back(pair);
  //      }
  //    });
  //
  //    for (auto &pair : pairs) {
  //      RemoveEdge(pair.first, pair.second);
  //    }
  //  }

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn == nullptr) {
    return false;
  }

  if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> request_queue;
  auto txn_id = txn->GetTransactionId();
  std::unique_lock<std::mutex> rows_lock(row_lock_map_latch_);
  if (!run_) {
    return false;
  }

  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid].reset(new LockRequestQueue);
  }
  request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  rows_lock.unlock();

  LockRequest *request{nullptr};
  if (request_queue->locked_requests_.count(txn_id) != 0) {
    request = request_queue->locked_requests_[txn_id];

    if (request->lock_mode_ == lock_mode) {
      return true;
    }
    UpgradeLockRow(txn, request->lock_mode_, lock_mode, oid, rid, request_queue.get());
    request->lock_mode_ = lock_mode;
    request_queue->upgrading_ = txn_id;
    request_queue->locked_requests_.erase(txn_id);
  }

  LockRequest *new_request{nullptr};
  if (request_queue->upgrading_ == txn_id) {
    new_request = request;
  } else {
    new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  }

  // std::unique_lock<std::mutex> lock(request_queue->latch_);
  // request_queue->request_queue_.push_back(new_request);
  if (request_queue->locked_requests_.empty()) {
    if (request_queue->upgrading_ == txn_id || request_queue->request_queue_.empty()) {
      if (request_queue->upgrading_ == txn_id) {
        request_queue->upgrading_ = INVALID_TXN_ID;
      }
      request_queue->locked_requests_.insert({txn_id, new_request});
      if (lock_mode == LockMode::EXCLUSIVE) {
        auto exc_rows_set = txn->GetExclusiveRowLockSet();
        (*exc_rows_set)[oid].insert(rid);
      } else {
        auto shared_rows_set = txn->GetSharedRowLockSet();
        (*shared_rows_set)[oid].insert(rid);
      }
      return true;
    }
  }

  {
    std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
    wait_for_lock_map_[txn_id] = request_queue;
  }

  if (request_queue->upgrading_ == txn_id) {
    request_queue->request_queue_.push_front(new_request);
  } else {
    request_queue->request_queue_.push_back(new_request);
  }

  TransactionState state;
  request_queue->cv_.wait(lock, [&]() -> bool {
    txn->LockTxn();
    state = txn->GetState();
    // txn->UnlockTxn();
    if (state == TransactionState::ABORTED) {
      txn->UnlockTxn();
      return true;
    }
    txn->UnlockTxn();

    if (!IsCompatible(request_queue->request_queue_.front()->lock_mode_, request_queue,
                      request_queue->request_queue_.front())) {
      return false;
    }
    //    if (request_queue->upgrading_ != INVALID_TXN_ID && request_queue->upgrading_ != txn_id) {
    //      return false;
    //    }
    return IsCompatible(lock_mode, request_queue, new_request);
  });

  {
    std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
    wait_for_lock_map_.erase(txn_id);
  }

  if (state == TransactionState::ABORTED) {
    auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
    if (iter != request_queue->request_queue_.end()) {
      request_queue->request_queue_.erase(iter);
    }

    if (request_queue->upgrading_ == txn_id) {
      request_queue->upgrading_ = INVALID_TXN_ID;
    }

    // txn_manager_->Abort(txn);
    delete new_request;
    request_queue->cv_.notify_all();
    return false;
  }

  if (request_queue->upgrading_ == txn_id) {
    request_queue->upgrading_ = INVALID_TXN_ID;
    request_queue->request_queue_.pop_front();
  } else {
    auto iter = std::find(request_queue->request_queue_.begin(), request_queue->request_queue_.end(), new_request);
    if (iter != request_queue->request_queue_.end()) {
      request_queue->request_queue_.erase(iter);
    }
  }
  request_queue->locked_requests_.insert({txn_id, new_request});
  // request_queue->cv_.notify_all();

  if (lock_mode == LockMode::EXCLUSIVE) {
    auto exc_rows_set = txn->GetExclusiveRowLockSet();
    (*exc_rows_set)[oid].insert(rid);
  } else {
    auto shared_rows_set = txn->GetSharedRowLockSet();
    (*shared_rows_set)[oid].insert(rid);
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  if (txn == nullptr) {
    return false;
  }

  std::shared_ptr<LockRequestQueue> request_queue;
  auto txn_id = txn->GetTransactionId();

  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if ((*s_row_lock_set)[oid].count(rid) == 0 && (*x_row_lock_set)[oid].count(rid) == 0) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  std::unique_lock<std::mutex> rows_lock(row_lock_map_latch_);
  request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(request_queue->latch_);
  rows_lock.unlock();

  //  if (request_queue->locked_requests_.count(txn_id) == 0) {
  //    txn->LockTxn();
  //    txn->SetState(TransactionState::ABORTED);
  //    txn->UnlockTxn();
  //    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  //  }

  auto request = request_queue->locked_requests_[txn_id];
  txn->LockTxn();
  auto txn_state = txn->GetState();

  auto lock_mode = request->lock_mode_;
  if (txn_state != TransactionState::ABORTED) {
    if (!force) {
      auto isolation_level = txn->GetIsolationLevel();
      if (isolation_level == IsolationLevel::REPEATABLE_READ) {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (isolation_level == IsolationLevel::READ_COMMITTED ||
                 isolation_level == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
    }
  }
  txn->UnlockTxn();

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      break;
    default:
      break;
  }

  // auto *req = request_queue->locked_requests_[txn_id];
  request_queue->locked_requests_.erase(txn_id);
  delete request;
  request_queue->cv_.notify_all();
  lock.unlock();

  //  {
  //    std::lock_guard<std::mutex> wlock(waits_for_latch_);
  //    std::vector<std::pair<txn_id_t, txn_id_t>> pairs;
  //    std::for_each(wait_pairs_.begin(), wait_pairs_.end(), [&](const std::pair<txn_id_t, txn_id_t> &pair) {
  //      if (pair.second == txn_id || pair.first == txn_id) {
  //        pairs.push_back(pair);
  //      }
  //    });
  //
  //    for (auto &pair : pairs) {
  //      RemoveEdge(pair.first, pair.second);
  //    }
  //  }

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  {
    std::scoped_lock<std::mutex, std::mutex> lock(table_lock_map_latch_, row_lock_map_latch_);
    run_ = false;
  }

  for (auto &[rid, req_queue] : row_lock_map_) {
    std::lock_guard<std::mutex> qu_lock(req_queue->latch_);
    for (auto [txn_id, req] : req_queue->locked_requests_) {
      delete req;
    }

    while (!req_queue->request_queue_.empty()) {
      auto *req = req_queue->request_queue_.front();
      req_queue->request_queue_.pop_front();
      delete req;
    }
  }

  for (auto &[rid, req_queue] : table_lock_map_) {
    std::lock_guard<std::mutex> qu_lock(req_queue->latch_);
    for (auto [txn_id, req] : req_queue->locked_requests_) {
      delete req;
    }

    while (!req_queue->request_queue_.empty()) {
      auto *req = req_queue->request_queue_.front();
      req_queue->request_queue_.pop_front();
      delete req;
    }
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) != waits_for_[t1].end()) {
    return;
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t>::iterator iter;
  if (waits_for_.count(t1) == 0 ||
      (iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2)) == waits_for_[t1].end()) {
    return;
  }

  waits_for_[t1].erase(iter);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> source_txns;
  for (auto &from_txn_info : waits_for_) {
    source_txns.push_back(from_txn_info.first);
  }
  std::sort(source_txns.begin(), source_txns.end());

  for (const auto src_txn_id : source_txns) {
    std::unordered_set<txn_id_t> on_path;
    on_path.insert(src_txn_id);
    std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> pairs;
    auto res = FindCycle(src_txn_id, on_path, txns_, pairs);
    //    for (auto pair : pairs) {
    //      auto iter = std::find(waits_for_[pair.first].begin(), waits_for_[pair.first].end(), pair.second);
    //      waits_for_[pair.first].erase(iter);
    //    }
    if (res) {
      txn_id_t max_id = INVALID_TXN_ID;
      for (auto id : on_path) {
        max_id = std::max(id, max_id);
      }
      BUSTUB_ASSERT(max_id != INVALID_TXN_ID, "Valid max txn id should't be -1");
      *txn_id = max_id;
      return res;
    }
  }

  //  for (auto txn : txns) {
  //    txn->UnlockTxn();
  //  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  //  {
  //    std::scoped_lock<std::mutex, std::mutex> two_map_lock(table_lock_map_latch_, row_lock_map_latch_);
  //    for (auto &[oid, req_queue] : table_lock_map_) {
  //      std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
  //      for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
  //        auto locked_txn_id = locked_req->txn_id_;
  //        std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
  //                      [&](const LockRequest *waited_req) {
  //                        res.push_back({waited_req->txn_id_, locked_txn_id});
  //                      });
  //      }
  //    }
  //
  //    for (auto &[oid, req_queue] : row_lock_map_) {
  //      std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
  //      for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
  //        auto locked_txn_id = locked_req->txn_id_;
  //        std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
  //                      [&](const LockRequest *waited_req) { AddEdge(waited_req->txn_id_, locked_txn_id); });
  //      }
  //    }
  //  }

  for (auto &[from_id, to_id_vec] : waits_for_) {
    for (auto to_id : to_id_vec) {
      res.emplace_back(from_id, to_id);
    }
  }

  lock.unlock();
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::unique_lock<std::mutex> wait_for_lock(waits_for_latch_);
      waits_for_.clear();
      //      {
      //        std::scoped_lock<std::mutex, std::mutex> two_map_lock(table_lock_map_latch_, row_lock_map_latch_);
      //        for (auto &[oid, req_queue] : table_lock_map_) {
      //          std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
      //          for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
      //            auto locked_txn_id = locked_req->txn_id_;
      //            for (auto &req : req_queue->request_queue_) {
      //              AddEdge(req->txn_id_, locked_txn_id);
      //            }
      //            //            std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
      //            //                          [&](const LockRequest *waited_req) { AddEdge(waited_req->txn_id_,
      //            //                          locked_txn_id); });
      //          }
      //        }
      //
      //        for (auto &[oid, req_queue] : row_lock_map_) {
      //          std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
      //          for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
      //            auto locked_txn_id = locked_req->txn_id_;
      //            for (auto &req : req_queue->request_queue_) {
      //              AddEdge(req->txn_id_, locked_txn_id);
      //            }
      //            //            std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
      //            //                          [&](const LockRequest *waited_req) { AddEdge(waited_req->txn_id_,
      //            //                          locked_txn_id); });
      //          }
      //        }
      //      }

      std::scoped_lock<std::mutex, std::mutex> two_map_lock(table_lock_map_latch_, row_lock_map_latch_);
      for (auto &[oid, req_queue] : table_lock_map_) {
        std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
        for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
          auto locked_txn_id = locked_req->txn_id_;
          for (auto &req : req_queue->request_queue_) {
            AddEdge(req->txn_id_, locked_txn_id);
          }
          //            std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
          //                          [&](const LockRequest *waited_req) { AddEdge(waited_req->txn_id_,
          //                          locked_txn_id); });
        }
      }

      for (auto &[oid, req_queue] : row_lock_map_) {
        std::unique_lock<std::mutex> queue_lock(req_queue->latch_);
        for (auto [txn_id, locked_req] : req_queue->locked_requests_) {
          auto locked_txn_id = locked_req->txn_id_;
          for (auto &req : req_queue->request_queue_) {
            AddEdge(req->txn_id_, locked_txn_id);
          }
          //            std::for_each(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
          //                          [&](const LockRequest *waited_req) { AddEdge(waited_req->txn_id_,
          //                          locked_txn_id); });
        }
      }

      for (auto &[txn_id, txn_vec] : waits_for_) {
        sort(txn_vec.begin(), txn_vec.end());
      }
      //      std::unordered_set<txn_id_t> aborted_txns;
      //      std::vector<std::pair<txn_id_t, txn_id_t>> need_delete_pairs;
      //      for (auto &pair : wait_pairs_) {
      //        if (aborted_txns.count(pair.first) != 0 || aborted_txns.count(pair.second) != 0) {
      //          continue;
      //        }
      //
      //        if (txns_.count(pair.first) == 0) {
      //          auto *txn = txn_manager_->GetTransaction(pair.first);
      //          txn->LockTxn();
      //          if (txn->GetState() != TransactionState::ABORTED) {
      //            txns_.insert({pair.first, txn});
      //          } else {
      //            aborted_txns.insert(pair.first);
      //            txn->UnlockTxn();
      //            continue;
      //          }
      //        }
      //
      //        if (txns_.count(pair.second) == 0) {
      //          auto *txn = txn_manager_->GetTransaction(pair.second);
      //          txn->LockTxn();
      //          if (txn->GetState() != TransactionState::ABORTED) {
      //            txns_.insert({pair.second, txn});
      //          } else {
      //            aborted_txns.insert(pair.second);
      //            txn->UnlockTxn();
      //            continue;
      //          }
      //        }
      //
      //        waits_for_[pair.first].push_back(pair.second);
      //      }
      //
      //      for (auto &txn : txns_) {
      //        txn.second->UnlockTxn();
      //      }

      //      for (const auto &from_txn_info : waits_for_) {
      //        auto *txn = txn_manager_->GetTransaction(from_txn_info.first);
      //        txn->LockTxn();
      //        if (txn->GetState() == TransactionState::ABORTED) {
      //          aborted_txns.insert(from_txn_info.first);
      //          txn->UnlockTxn();
      //        } else {
      //          txns_.insert({from_txn_info.first, txn});
      //        }
      //      }
      //
      //      for (auto txn : aborted_txns) {
      //        waits_for_.erase(txn);
      //      //      }

      txn_id_t need_abort_txn_id = INVALID_TXN_ID;
      while (HasCycle(&need_abort_txn_id)) {
        auto *need_abort_txn = txn_manager_->GetTransaction(need_abort_txn_id);
        need_abort_txn->UnlockTxn();
        need_abort_txn->SetState(TransactionState::ABORTED);
        need_abort_txn->UnlockTxn();

        std::shared_ptr<LockRequestQueue> request_queue;
        std::unique_lock<std::mutex> wait_lock(wait_for_map_latch_);
        if (wait_for_lock_map_.count(need_abort_txn_id) != 0) {
          request_queue = wait_for_lock_map_[need_abort_txn_id];
        } else {
          if (waits_for_.count(need_abort_txn_id) != 0) {
            waits_for_.erase(need_abort_txn_id);
          }

          for (auto &waits : waits_for_) {
            auto iter = std::find(waits.second.begin(), waits.second.end(), need_abort_txn_id);
            if (iter != waits.second.end()) {
              waits.second.erase(iter);
            }
          }

          need_abort_txn_id = INVALID_TXN_ID;
          // need_abort_txn->UnlockTxn();
          continue;
        }
        std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
        wait_lock.unlock();
        request_queue->cv_.notify_all();
        queue_lock.unlock();

        //        request_queue->is_abort_ = true;
        //        request_queue->need_abort_ = need_abort_txn_id;
        //        request_queue->wait_request_nums_ = request_queue->request_queue_.size();
        //        request_queue->cv_.notify_all();
        //        request_queue->all_recv_cv_.wait(queue_lock, [&]() { return request_queue->wait_request_nums_ == 0;
        //        }); request_queue->is_abort_ = false; request_queue->need_abort_ = INVALID_TXN_ID;
        // txn_manager_->Abort(need_abort_txn);

        if (waits_for_.count(need_abort_txn_id) != 0) {
          waits_for_.erase(need_abort_txn_id);
        }

        for (auto &waits : waits_for_) {
          auto iter = std::find(waits.second.begin(), waits.second.end(), need_abort_txn_id);
          if (iter != waits.second.end()) {
            waits.second.erase(iter);
          }
        }
        need_abort_txn_id = INVALID_TXN_ID;
        // need_abort_txn->UnlockTxn();
      }
    }
  }
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  txn->LockTxn();
  auto txn_state = txn->GetState();

  if (txn_state == TransactionState::ABORTED) {
    txn->UnlockTxn();
    return false;
  }

  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (txn_state == TransactionState::SHRINKING) {
      // txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (txn_state == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      // txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      // txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }

    //    else if (txn_state == TransactionState::SHRINKING) {
    //      txn->LockTxn();
    //      txn->SetState(TransactionState::ABORTED);
    //      txn->UnlockTxn();
    //      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);

    if (txn_state == TransactionState::SHRINKING) {
      // txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockManager::LockMode lock_mode,
                                   LockManager::LockMode new_lock_mode, const LockRequestQueue *qu,
                                   const table_oid_t &oid) -> bool {
  CheckLocksCompatible(txn, lock_mode, new_lock_mode);

  if (qu->upgrading_ != INVALID_TXN_ID) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    default:
      break;
  }

  //  switch (new_lock_mode) {
  //    case LockMode::INTENTION_SHARED:
  //      txn->GetIntentionSharedTableLockSet()->insert(oid);
  //      break;
  //    case LockMode::SHARED:
  //      txn->GetSharedTableLockSet()->insert(oid);
  //      break;
  //    case LockMode::INTENTION_EXCLUSIVE:
  //      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  //      break;
  //    case LockMode::SHARED_INTENTION_EXCLUSIVE:
  //      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  //      break;
  //    case LockMode::EXCLUSIVE:
  //      txn->GetExclusiveTableLockSet()->insert(oid);
  //      break;
  //    default:
  //      break;
  //  }
  return true;
}

auto LockManager::AreLocksCompatible(LockManager::LockMode l1, LockManager::LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED:
      return true;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 == LockMode::EXCLUSIVE || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    default:
      break;
  }

  return false;
}

auto LockManager::CheckLocksCompatible(Transaction *txn, LockManager::LockMode l1, LockManager::LockMode l2) -> void {
  if (!AreLocksCompatible(l1, l2)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockManager::LockMode lock_mode, LockManager::LockMode new_lock_mode,
                                 const table_oid_t &oid, const RID &rid, const LockRequestQueue *qu) -> bool {
  CheckLocksCompatible(txn, lock_mode, new_lock_mode);

  if (qu->upgrading_ != INVALID_TXN_ID) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // BUSTUB_ASSERT(lock_mode == LockMode::SHARED, "The upgrade row lock mode must be shared!");
  // txn->LockTxn();
  auto shared_rows_set = txn->GetSharedRowLockSet();
  (*shared_rows_set)[oid].erase(rid);
  // txn->UnlockTxn();
  return true;
}

auto LockManager::IsTxnAborted(txn_id_t txn_id) -> bool {
  auto *txn = txn_manager_->GetTransaction(txn_id);
  txn->LockTxn();
  auto state = txn->GetState();
  txn->UnlockTxn();

  return state == TransactionState::ABORTED;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_map<txn_id_t, Transaction *> &txns,
                            std::unordered_set<std::pair<txn_id_t, txn_id_t>, WaitPairHashFunction> &pairs) -> bool {
  if (waits_for_.count(source_txn) == 0) {
    return false;
  }

  auto &to_txns = waits_for_[source_txn];

  int size = to_txns.size();
  for (int i = 0; i < size; ++i) {
    auto txn_id = to_txns[i];
    if (on_path.count(txn_id) != 0) {
      return true;
    }

    //    if (pairs.count({source_txn, txn_id}) != 0) {
    //      continue;
    //    }
    //
    //    auto *txn = txn_manager_->GetTransaction(txn_id);
    //    if (txns.count(txn) == 0 && IsTxnAborted(txn_id)) {
    //      pairs.insert({source_txn, txn_id});
    //      continue;
    //    }

    on_path.insert(txn_id);
    auto res = FindCycle(txn_id, on_path, txns, pairs);
    if (res) {
      return res;
    }
    on_path.erase(txn_id);
  }

  return false;
}

auto LockManager::IsCompatible(LockManager::LockMode l1, std::shared_ptr<LockRequestQueue> &req_queue,
                               LockRequest *cur_req) -> bool {
  for (auto &[txn_id, req] : req_queue->locked_requests_) {
    LockManager::LockMode l2 = req->lock_mode_;
    if (!IsLockModeCompatible(l1, l2)) {
      return false;
    }
  }

  for (auto &iter : req_queue->request_queue_) {
    if (iter == cur_req) {
      break;
    }

    LockManager::LockMode l2 = iter->lock_mode_;
    if (!IsLockModeCompatible(l1, l2)) {
      return false;
    }
  }
  return true;
}

auto LockManager::IsLockModeCompatible(LockManager::LockMode l1, LockManager::LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED:
      if (l2 != LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED:
      if (l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
    case LockMode::EXCLUSIVE:
    default:
      break;
  }
  return false;
}

}  // namespace bustub
