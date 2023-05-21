#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (auto &[type, expr] : plan_->order_bys_) {
      auto lhs_value = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      auto rhs_value = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_value.CompareEquals(rhs_value) == CmpBool::CmpTrue) {
        continue;
      }

      if (type == OrderByType::DESC) {
        return lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue;
      }
      return rhs_value.CompareGreaterThan(lhs_value) == CmpBool::CmpTrue;
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_pos_ != tuples_.size()) {
    *tuple = tuples_[cur_pos_];
    *rid = tuple->GetRid();
    ++cur_pos_;
    return true;
  }
  return false;
}

}  // namespace bustub
