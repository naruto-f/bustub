//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_)),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    flag_ = true;
    TupleMeta meta;
    meta.is_deleted_ = true;
    meta.delete_txn_id_ = INVALID_TXN_ID;
    meta.insert_txn_id_ = INVALID_TXN_ID;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    for (auto *index_info : index_infos_) {
      std::vector<uint32_t> key_attrs;
      for (auto &col : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      index_info->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs),
                                      *rid, nullptr);
    }
    ++count;
  }

  if (count == 0) {
    if (!flag_) {
      *tuple = Tuple{std::vector<Value>{Value{INTEGER, count}}, &GetOutputSchema()};
      flag_ = true;
      return true;
    }
    return false;
  }
  *tuple = Tuple{std::vector<Value>{Value{INTEGER, count}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
