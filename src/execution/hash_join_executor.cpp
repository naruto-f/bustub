//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_child_->Init();
  right_child_->Init();

  Tuple tuple;
  RID rid;

  while (left_child_->Next(&tuple, &rid)) {
    left_hash_map_.insert({GetLeftJoinKey(&tuple), tuple});
  }

  while (right_child_->Next(&tuple, &rid)) {
    right_hash_map_.insert({GetRightJoinKey(&tuple), tuple});
  }

  left_iterator_ = left_hash_map_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!qu_.empty()) {
    *tuple = qu_.front();
    qu_.pop();
    return true;
  }

  while (left_iterator_ != left_hash_map_.cend()) {
    auto [iter_beg, iter_end] = right_hash_map_.equal_range(left_iterator_->first);
    if (iter_beg == right_hash_map_.end()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_iterator_->second.GetValue(&left_child_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
      } else {
        ++left_iterator_;
        continue;
      }
    } else {
      bool flag = false;
      for (; iter_beg != iter_end; ++iter_beg) {
        // plan_->GetLeftJoinKey(&left_iterator_->second) == plan_->GetRightJoinKey(&iter_beg->second)
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_iterator_->second.GetValue(&left_child_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(iter_beg->second.GetValue(&right_child_->GetOutputSchema(), i));
        }

        if (!flag) {
          *tuple = Tuple{values, &GetOutputSchema()};
          flag = true;
        } else {
          qu_.push(Tuple{values, &GetOutputSchema()});
        }
      }
      if (!flag) {
        ++left_iterator_;
        continue;
      }
    }
    ++left_iterator_;
    return true;
  }

  return false;
}

}  // namespace bustub
