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
#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      hash_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      iter_(hash_.Begin()) {}

void AggregationExecutor::Init() {
  hash_.GenerateInitialAggregateValue();
  assert(child_);
  assert(plan_);
  child_->Init();

  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_->Next(&tmp_tuple, &tmp_rid)) {
    hash_.InsertCombine(MakeAggregateKey(&tmp_tuple), MakeAggregateValue(&tmp_tuple));
  }

  iter_ = hash_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == hash_.End()) {
    return false;
  }
  do {
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(iter_.Key().group_bys_, iter_.Val().aggregates_).GetAs<bool>()) {
      std::vector<Value> tmp;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        tmp.emplace_back(col.GetExpr()->EvaluateAggregate(iter_.Key().group_bys_, iter_.Val().aggregates_));
      }
      *tuple = Tuple(tmp, GetOutputSchema());
      return true;
    }
    ++iter_;
  } while (iter_ != hash_.End());

  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
