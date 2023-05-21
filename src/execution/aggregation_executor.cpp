//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }

  if (aht_.Begin() == aht_.End()) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    //    auto clouse = aht_iterator_.Key().group_bys_;
    //    auto aggregate = aht_iterator_.Val().aggregates_;
    //    clouse.insert(clouse.end(), aggregate.begin(), aggregate.end());
    *tuple = Tuple{aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
    *rid = tuple->GetRid();
    flag_ = true;
    return true;
  }

  if (aht_iterator_ != aht_.End()) {
    auto clouse = aht_iterator_.Key().group_bys_;
    auto aggregate = aht_iterator_.Val().aggregates_;
    clouse.insert(clouse.end(), aggregate.begin(), aggregate.end());
    *tuple = Tuple{clouse, &GetOutputSchema()};
    *rid = tuple->GetRid();
    ++aht_iterator_;
    return true;
    // aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end()
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
