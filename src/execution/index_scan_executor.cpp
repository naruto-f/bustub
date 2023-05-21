//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_.IsEnd()) {
    auto [tuple_meta, t] = table_info_->table_->GetTuple(iter_.operator*().second);
    if (!tuple_meta.is_deleted_) {
      *tuple = t;
      *rid = iter_.operator*().second;
      ++iter_;
      return true;
    }
    ++iter_;
  }
  return false;
}

}  // namespace bustub
