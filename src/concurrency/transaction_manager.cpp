//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto txn_id = txn->GetTransactionId();

  // txn->LockTxn();
  //  if (txn->IsUnlock()) {
  //    txn->UnlockTxn();
  //    return;
  //  }
  //
  auto table_write_records = txn->GetWriteSet();
  while (!table_write_records->empty()) {
    auto record = table_write_records->back();
    table_write_records->pop_back();
    auto write_type = record.wtype_;
    if (write_type == WType::INSERT) {
      TupleMeta meta;
      meta.is_deleted_ = true;
      meta.delete_txn_id_ = txn_id;
      meta.insert_txn_id_ = INVALID_TXN_ID;
      record.table_heap_->UpdateTupleMeta(meta, record.rid_);
    } else if (write_type == WType::DELETE) {
      TupleMeta meta;
      meta.is_deleted_ = false;
      meta.delete_txn_id_ = INVALID_TXN_ID;
      meta.insert_txn_id_ = txn_id;
      record.table_heap_->UpdateTupleMeta(meta, record.rid_);

      //      auto [tuple_meta, tuple] = record.table_heap_->GetTuple(record.rid_);
      //      tuple_meta.is_deleted_ = false;
      //      tuple_meta.delete_txn_id_ = INVALID_TXN_ID;
      //      tuple_meta.insert_txn_id_ = txn_id;
      //
      //      record.table_heap_->InsertTuple(tuple_meta, tuple, lock_manager_, txn);
    } else {
    }
  }

  // txn->SetUnlock();
  // txn->UnlockTxn();

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
