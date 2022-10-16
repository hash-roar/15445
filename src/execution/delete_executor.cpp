//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }

  table_info_ = catalog->GetTable(plan_->TableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception("table not exist");
  }
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  assert(child_executor_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  while (child_executor_->Next(&tmp_tuple, rid)) {
    auto success = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (auto index : indexs_) {
      index->index_->DeleteEntry(tmp_tuple, *rid, exec_ctx_->GetTransaction());
    }
    if (!success) {
      throw Exception("delete error");
    }
  }

  return false;
}

}  // namespace bustub
