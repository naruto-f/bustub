//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    //    TupleMeta meta;
    //    meta.is_deleted_ = true;
    //    meta.delete_txn_id_ = INVALID_TXN_ID;
    //    meta.insert_txn_id_ = INVALID_TXN_ID;
    //    table_info_->table_->UpdateTupleMeta(meta, *rid);

    // std::vector<Value> values(GetOutputSchema().GetColumnCount());
    std::vector<Value> values;
    for (const auto &target_expression : plan_->target_expressions_) {
      //      auto *expression = dynamic_cast<ColumnValueExpression*>(target_expression.get());
      //      values[expression->GetColIdx()] = expression->Evaluate(tuple, GetOutputSchema());
      values.push_back(target_expression->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple(values, &table_info_->schema_);
    // plan_->target_expressions_->Evaluate(tuple, GetOutputSchema())

    for (auto *index_info : index_infos_) {
      std::vector<uint32_t> key_attrs;
      for (auto &col : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      index_info->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs),
                                      *rid, nullptr);
    }

    TupleMeta meta;
    meta.is_deleted_ = true;
    meta.delete_txn_id_ = INVALID_TXN_ID;
    meta.insert_txn_id_ = INVALID_TXN_ID;
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    meta.is_deleted_ = false;
    auto new_rid = table_info_->table_->InsertTuple(meta, new_tuple);
    for (auto *index_info : index_infos_) {
      std::vector<uint32_t> key_attrs;
      for (auto &col : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      index_info->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, key_attrs),
                                      new_rid.value(), nullptr);
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
  flag_ = true;

  return true;
}

}  // namespace bustub
