//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeEagerIterator()),
      txn_(exec_ctx_->GetTransaction()) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto isolation_level = txn_->GetIsolationLevel();
  auto is_delete = exec_ctx_->IsDelete();
  bool lock_success{true};
  if (is_delete) {
    lock_success =
        exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
  } else {
    if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) {
      lock_success =
          exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
    }
  }

  if (!lock_success) {
    throw ExecutionException("Table lock field!");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }

  auto tuple_info = iter_.GetTuple();
  while (tuple_info.first.is_deleted_) {
    ++iter_;
    if (iter_.IsEnd()) {
      return false;
    }

    tuple_info = iter_.GetTuple();
  }

  auto isolation_level = txn_->GetIsolationLevel();
  auto is_delete = exec_ctx_->IsDelete();
  bool lock_success{true};
  if (is_delete) {
    lock_success =
        exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, iter_.GetRID());
  } else {
    if (isolation_level != IsolationLevel::READ_UNCOMMITTED) {
      lock_success =
          exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, plan_->table_oid_, iter_.GetRID());
    }
  }
  if (!lock_success) {
    throw ExecutionException("Row lock field!");
  }
  //  else {
  //    //    if (is_delete) {
  //    //      txn_->AppendTableWriteRecord({plan_->table_oid_, iter_.GetRID(),
  //    //      exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_.get()});
  //    //    }
  //  }

  *tuple = tuple_info.second;
  *rid = iter_.GetRID();

  // TODO(naruto): implement filter pushdown to scan
  if (!is_delete) {
    if (isolation_level == IsolationLevel::READ_COMMITTED) {
      lock_success = exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->table_oid_, iter_.GetRID());
    }
  }

  if (!lock_success) {
    throw ExecutionException("Row unlock field!");
  }

  ++iter_;
  return true;
}

}  // namespace bustub
