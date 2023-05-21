//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  Tuple tuple;
  RID rid;

  right_executor_->Init();
  left_executor_->Init();

  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }

  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }

  left_iter_ = left_tuples_.cbegin();
  right_iter_ = right_tuples_.cbegin();
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  //  Tuple tuple;
  //  RID rid;
  //
  //  right_executor_->Init();
  //  left_executor_->Init();
  //
  //  while (left_executor_->Next(&tuple, &rid)) {
  //    left_tuples_.push_back(tuple);
  //  }
  //
  //  while (right_executor_->Next(&tuple, &rid)) {
  //    right_tuples_.push_back(tuple);
  //  }
  //
  //  left_iter_ = left_tuples_.cbegin();
  //  right_iter_ = right_tuples_.cbegin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (left_iter_ == left_tuples_.cend()) {
    return false;
  }

  auto filter_expr = plan_->Predicate();
  while (left_iter_ != left_tuples_.cend()) {
    bool valid = false;
    if (right_iter_ == right_tuples_.cend()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_iter_->GetValue(&left_executor_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        ++left_iter_;
        right_executor_->Init();
        right_iter_ = right_tuples_.cbegin();
        return true;
      }
    }

    auto value = filter_expr->EvaluateJoin(left_iter_.base(), left_executor_->GetOutputSchema(), right_iter_.base(),
                                           right_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_iter_->GetValue(&left_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(right_iter_->GetValue(&right_executor_->GetOutputSchema(), i));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      flag_ = true;
      valid = true;
    }

    auto old_right_iter = right_iter_;
    ++right_iter_;
    if (right_iter_ == right_tuples_.cend()) {
      if (!flag_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_iter_->GetValue(&left_executor_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(
              old_right_iter->GetValue(&right_executor_->GetOutputSchema(), i).GetTypeId()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        valid = true;
      }

      ++left_iter_;
      right_executor_->Init();
      right_iter_ = right_tuples_.cbegin();
      flag_ = false;
    }

    if (valid) {
      return true;
    }
  }

  return false;
}

}  // namespace bustub
