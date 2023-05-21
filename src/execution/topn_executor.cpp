#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  auto compare = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (auto &[type, expr] : plan_->order_bys_) {
      auto lhs_value = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      auto rhs_value = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_value.CompareEquals(rhs_value) == CmpBool::CmpTrue) {
        continue;
      }

      if (type == OrderByType::DESC) {
        return rhs_value.CompareGreaterThan(lhs_value) == CmpBool::CmpFalse;
      }
      return rhs_value.CompareGreaterThan(lhs_value) == CmpBool::CmpTrue;
    }
    return true;
  };

  child_executor_->Init();
  std::vector<Tuple> tuples;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.push_back(tuple);
    push_heap(tuples.begin(), tuples.end(), compare);
    if (tuples.size() > plan_->n_) {
      pop_heap(tuples.begin(), tuples.end(), compare);
      tuples.pop_back();
    }
  }

  while (!tuples.empty()) {
    tuples_.push_back(tuples[0]);
    pop_heap(tuples.begin(), tuples.end(), compare);
    tuples.pop_back();
  }

  std::reverse(tuples_.begin(), tuples_.end());
  cur_heap_num_ = tuples_.size();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_pos_ != tuples_.size()) {
    *tuple = tuples_[cur_pos_];
    *rid = tuple->GetRid();
    ++cur_pos_;
    --cur_heap_num_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return cur_heap_num_;
};

}  // namespace bustub
