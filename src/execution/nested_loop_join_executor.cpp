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
#include <cassert>
#include <utility>
#include "catalog/schema.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }
  assert(left_executor_);
  assert(right_executor_);
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto left_plan = plan_->GetLeftPlan();
  auto right_plan = plan_->GetRightPlan();

  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto result = plan_->Predicate()->EvaluateJoin(&left_tuple, left_plan->OutputSchema(), &right_tuple,
                                                     right_plan->OutputSchema());
      if (result.GetAs<bool>()) {
        *tuple = result.GetAs<Tuple>();
        *rid = tuple->GetRid();
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub
