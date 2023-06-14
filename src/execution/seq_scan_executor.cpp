
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
#include "concurrency/transaction_manager.h"

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
    try {
      lock_success =
          exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    } catch (const TransactionAbortException &error) {
      throw ExecutionException("Table lock field!");
    }
    //    lock_success =
    //        exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE,
    //        plan_->table_oid_);
  } else {
    if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) {
      auto ie_table_set = txn_->GetIntentionExclusiveTableLockSet();
      if ((*ie_table_set).count(plan_->table_oid_) == 0) {
        try {
          lock_success =
              exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
        } catch (const TransactionAbortException &error) {
          throw ExecutionException("Table lock field!");
        }
      }
      //      try {
      //        lock_success =
      //            exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED,
      //            plan_->table_oid_);
      //      } catch (const TransactionAbortException &error) {
      //        throw ExecutionException("Table lock field!");
      //      }
      //      lock_success =
      //          exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED,
      //          plan_->table_oid_);
    }
  }

  if (!lock_success) {
    throw ExecutionException("Table lock field!");
  }

  auto iter{exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeEagerIterator()};
  while (!iter.IsEnd()) {
    bool flag{false};
    auto exc_rows_set = txn_->GetExclusiveRowLockSet();
    if ((*exc_rows_set)[plan_->table_oid_].count(iter.GetRID()) == 0) {
      flag = true;
    }

    if (is_delete) {
      try {
        lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_,
                                                            iter.GetRID());
      } catch (const TransactionAbortException &error) {
        throw ExecutionException("Table lock field!");
      }
    } else {
      if (isolation_level != IsolationLevel::READ_UNCOMMITTED && flag) {
        try {
          lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, plan_->table_oid_,
                                                              iter.GetRID());
        } catch (const TransactionAbortException &error) {
          throw ExecutionException("Table lock field!");
        }
        if (isolation_level == IsolationLevel::READ_COMMITTED) {
          need_unlock_.push_back(iter.GetRID());
        }
      }
    }
    if (!lock_success) {
      throw ExecutionException("Row lock field!");
    }

    ++iter;
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    for (auto &id : need_unlock_) {
      try {
        exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->table_oid_, id);
      } catch (const TransactionAbortException &error) {
        throw ExecutionException("Table lock field!");
      }
    }
    return false;
  }

  // auto isolation_level = txn_->GetIsolationLevel();
  // auto is_delete = exec_ctx_->IsDelete();
  // bool lock_success{true};

  auto tuple_info = iter_.GetTuple();
  while (tuple_info.first.is_deleted_) {
    //    if (!is_delete && isolation_level == IsolationLevel::READ_COMMITTED) {
    //      need_unlock_.push_back(iter_.GetRID());
    //    }

    //    bool flag{false};
    //    auto exc_rows_set = txn_->GetExclusiveRowLockSet();
    //    if ((*exc_rows_set)[plan_->table_oid_].count(iter_.GetRID()) == 0) {
    //      flag = true;
    //    }
    //
    //    if (is_delete) {
    //      if (flag) {
    //        try {
    //          lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE,
    //          plan_->table_oid_,
    //                                                              iter_.GetRID());
    //        } catch (const TransactionAbortException &error) {
    //          throw ExecutionException("Table lock field!");
    //        }
    //      }
    //    } else {
    //      //      auto txn_state = txn_->GetState();
    //      //      if (txn_state == TransactionState::SHRINKING) {
    //      //
    //      //
    //      //      }
    //
    //      if (isolation_level != IsolationLevel::READ_UNCOMMITTED && flag) {
    //        try {
    //          lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED,
    //          plan_->table_oid_,
    //                                                              iter_.GetRID());
    //        } catch (const TransactionAbortException &error) {
    //          throw ExecutionException("Table lock field!");
    //        }
    //        if (isolation_level == IsolationLevel::READ_COMMITTED) {
    //          need_unlock_.push_back(iter_.GetRID());
    //        }
    //      }
    //    }
    //    if (!lock_success) {
    //      throw ExecutionException("Row lock field!");
    //    }
    ++iter_;
    if (iter_.IsEnd()) {
      for (auto &id : need_unlock_) {
        try {
          exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->table_oid_, id);
        } catch (const TransactionAbortException &error) {
          throw ExecutionException("Table lock field!");
        }
      }
      return false;
    }

    tuple_info = iter_.GetTuple();
  }

  //  bool flag{false};
  //  auto exc_rows_set = txn_->GetExclusiveRowLockSet();
  //  if ((*exc_rows_set)[plan_->table_oid_].count(iter_.GetRID()) == 0) {
  //    flag = true;
  //  }
  //
  //  if (is_delete) {
  //    try {
  //      lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_,
  //                                                          iter_.GetRID());
  //    } catch (const TransactionAbortException &error) {
  //      throw ExecutionException("Table lock field!");
  //    }
  //  } else {
  //    if (isolation_level != IsolationLevel::READ_UNCOMMITTED && flag) {
  //      try {
  //        lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, plan_->table_oid_,
  //                                                            iter_.GetRID());
  //      } catch (const TransactionAbortException &error) {
  //        throw ExecutionException("Table lock field!");
  //      }
  //      if (isolation_level == IsolationLevel::READ_COMMITTED) {
  //        need_unlock_.push_back(iter_.GetRID());
  //      }
  //    }
  //  }
  //  if (!lock_success) {
  //    throw ExecutionException("Row lock field!");
  //  }

  //  else {
  //    //    if (is_delete) {
  //    //      txn_->AppendTableWriteRecord({plan_->table_oid_, iter_.GetRID(),
  //    //      exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_.get()});
  //    //    }
  //  }

  *tuple = tuple_info.second;
  *rid = iter_.GetRID();

  // TODO(naruto): implement filter pushdown to scan
  //  if ((*exc_rows_set)[plan_->table_oid_].count(iter_.GetRID()) == 0) {
  //    flag = true;
  //  }
  //
  //  if (!is_delete) {
  //    if (isolation_level == IsolationLevel::READ_COMMITTED && flag) {
  //      lock_success = exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->table_oid_, iter_.GetRID());
  //    }
  //  }
  //
  //  if (!lock_success) {
  //    throw ExecutionException("Row unlock field!");
  //  }

  ++iter_;
  return true;
}

}  // namespace bustub
