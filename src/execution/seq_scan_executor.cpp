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
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
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
  *tuple = tuple_info.second;
  *rid = iter_.GetRID();
  ++iter_;
  return true;
}

}  // namespace bustub
