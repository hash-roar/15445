//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <utility>

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { child_executor_->Init(); }

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  // check limit
  if (has_output_ >= plan_->GetLimit()) {
    return false;
  }

  // get next
  if (child_executor_->Next(tuple, rid)) {
    has_output_++;
    return true;
  }
  // no more next
  return false;
}

}  // namespace bustub
