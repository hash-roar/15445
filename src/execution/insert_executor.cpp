//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  //   assert(plan_->GetType() == PlanType::Insert);
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }
  table_info_ = catalog->GetTable(plan_->TableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception("table not exist");
  }
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::InsertOne(const Tuple &tuple) {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    for (const auto &value : values) {
      Tuple tmp_tuple(value, &table_info_->schema_);
      bool success = table_info_->table_->InsertTuple(tmp_tuple, rid, exec_ctx_->GetTransaction());
      // update index
      for (auto index : indexs_) {
        index->index_->InsertEntry(tmp_tuple, *rid, exec_ctx_->GetTransaction());
      }
      if (!success) {
        throw Exception("too large to insert");
      }
    }
  } else {
    // assert valid has been done
    if (!child_executor_) {
      return false;
    }
    child_executor_->Init();
    // get value form child
    Tuple tmp_tuple;
    while (child_executor_->Next(&tmp_tuple, rid)) {
      bool success = table_info_->table_->InsertTuple(tmp_tuple, rid, exec_ctx_->GetTransaction());
      for (auto index : indexs_) {
        index->index_->InsertEntry(tmp_tuple, *rid, exec_ctx_->GetTransaction());
      }
      if (!success) {
        throw Exception("too large to insert");
      }
    }
  }
  return false;
}

}  // namespace bustub
