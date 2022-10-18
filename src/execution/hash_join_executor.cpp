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
#include <cstddef>
#include <functional>
#include <vector>
#include "common/rid.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }
  assert(left_executor_);
  assert(right_executor_);
  left_executor_->Init();
  right_executor_->Init();

  // build the hash table
  Tuple tmp_tuple;
  RID tmp_rid;
  const AbstractExpression *left_expr = plan_->LeftJoinKeyExpression();
  while (left_executor_->Next(&tmp_tuple, &tmp_rid)) {
    HashJoinKey key;
    key.val_ = left_expr->Evaluate(&tmp_tuple, plan_->GetLeftPlan()->OutputSchema());
    hash_table_[key].emplace_back(tmp_tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  const AbstractExpression *right_expr = plan_->RightJoinKeyExpression();
  while (right_executor_->Next(&right_tuple, rid)) {
    HashJoinKey key;
    key.val_ = right_expr->Evaluate(&right_tuple, plan_->GetLeftPlan()->OutputSchema());
    if (hash_table_.count(key) == 0) {
      continue;
    }
    auto &bucket = hash_table_[key];
    std::vector<Value> temp{};
    for (auto &bucket : bucket) {
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        temp.emplace_back(col.GetExpr()->EvaluateJoin(&bucket, left_executor_->GetOutputSchema(), &right_tuple,
                                                      right_executor_->GetOutputSchema()));
      }
    }
    *tuple = Tuple(temp, GetOutputSchema());
  }

  return false;
}

}  // namespace bustub
