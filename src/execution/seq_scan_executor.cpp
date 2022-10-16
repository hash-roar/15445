//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cassert>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/rid.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  if (catalog == nullptr) {
    throw Exception("get catalog error");
  }

  table_info_ = catalog->GetTable(plan_->GetTableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception("table not exist");
  }
  // create table iter
  TableHeap *table_heap = catalog->GetTable(plan_->GetTableOid())->table_.get();
  iter_.emplace(table_heap->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  assert(iter_.has_value());

  while (iter_.value() != table_info_->table_->End()) {
    auto pred = plan_->GetPredicate();
    auto temp = *iter_.value();
    if (pred != nullptr) {
      if (!pred->Evaluate(&temp, &table_info_->schema_).GetAs<bool>()) {
        iter_.value()++;
        continue;
      }
    }
    *tuple = *iter_.value();
    *rid = tuple->GetRid();
    iter_.value()++;
    return true;
  }

  return false;
}

}  // namespace bustub
