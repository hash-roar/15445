//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>

#include "common/exception.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }

  table_info_ = catalog->GetTable(plan_->TableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception("table not exist");
  }
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  while (child_executor_->Next(&tmp_tuple, rid)) {
    auto new_tuple = GenerateUpdatedTuple(tmp_tuple);
    auto success = table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
    // update indexs
    for (auto index : indexs_) {
      index->index_->DeleteEntry(tmp_tuple, tmp_tuple.GetRid(), exec_ctx_->GetTransaction());
      index->index_->InsertEntry(new_tuple, *rid, exec_ctx_->GetTransaction());
    }
    if (!success) {
      throw Exception("update error");
    }
  }

  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
