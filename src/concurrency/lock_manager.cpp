//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include "concurrency/transaction.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // verify valid
  if (txn->GetState() == TransactionState::ABORTED || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
      txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  auto &lque = lock_table_[rid];

  // for that other thread may change the request list,we use iter here

  auto iter = lque.request_queue_.begin();

  while (iter != lque.request_queue_.end()) {
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock lock{latch_};
  txn->GetExclusiveLockSet()->emplace(rid);
  auto &lque = lock_table_[rid];
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (lock_table_.count(rid) == 0) {
    lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE, true);
  } else {
    auto &que = lock_table_[rid].request_queue_;

    bool has_lock = std::any_of(que.begin(), que.end(), [](const LockRequest &req) -> bool { return req.granted_; });
    if (has_lock) {
      lock_table_[rid].cv_.wait(lock);
      // get lock now
      lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE, true);
    }
  }

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
