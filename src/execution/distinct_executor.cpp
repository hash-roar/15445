//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include <cassert>
#include <utility>
#include "execution/plans/distinct_plan.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  assert(child_executor_);
  child_executor_->Init();
  Tuple tmp_tuple;
  RID tmp_rid;
  auto num_colum = GetOutputSchema()->GetColumnCount();
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    DistinctKey key;

    key.vals_.reserve(num_colum);
    for (size_t i = 0; i < num_colum; i++) {
      key.vals_.emplace_back(tmp_tuple.GetValue(GetOutputSchema(), i));
    }
    if (hash_.count(key) == 0) {
      hash_.emplace(key, tmp_tuple);
    }
  }

  iter_ = hash_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == hash_.end()) {
    return false;
  }

  *tuple = iter_->second;
  *rid = tuple->GetRid();
  iter_++;

  return true;
}

}  // namespace bustub
