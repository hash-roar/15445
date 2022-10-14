//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  auto key_hash = Hash(key);
  return key_hash & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  if (directory_page_id_ == INVALID_PAGE_ID) {
    auto new_pg = buffer_pool_manager_->NewPage(&directory_page_id_);
    assert(new_pg != nullptr);
    HashTableDirectoryPage *result = reinterpret_cast<HashTableDirectoryPage *>(new_pg->GetData());
    result->SetPageId(directory_page_id_);

    page_id_t new_bu_pg_id;
    auto new_bucket_pg = buffer_pool_manager_->NewPage(&new_bu_pg_id);
    assert(new_bucket_pg != nullptr);

    result->SetBucketPageId(0, new_bu_pg_id);
    result->SetLocalDepth(0, 0);
    buffer_pool_manager_->UnpinPage(new_bu_pg_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  }

  auto page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_pgid = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_pgid);

  bool success = bucket->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_pgid, false);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get bucket
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_pgid = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_pgid);

  // check not full
  if (!bucket->IsFull()) {
    bool success = bucket->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_pgid, true);
    return success;
  }

  // bucket full split bucket
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_pgid, true);
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();  // rel
  frame_id_t bucket_pgid = KeyToPageId(key, dir_page);
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_depth = dir_page->GetLocalDepth(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_pgid);
  auto dir_depth = dir_page->GetGlobalDepth();

  // if can not split any more
  if ((1 << (bucket_depth + 1)) > DIRECTORY_ARRAY_SIZE) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_pgid, false);
    return false;
  }

  // if need to grow directory

  if (bucket_depth == dir_depth) {
    dir_page->IncrGlobalDepth();
  }

  // get all old data from  old bucket
  auto old_bucket_data = bucket->GetAll();
  // create new image bucket
  page_id_t new_bucket_pgid;
  auto new_bk_pg = buffer_pool_manager_->NewPage(&new_bucket_pgid);
  assert(new_bk_pg != nullptr);
  HASH_TABLE_BUCKET_TYPE *new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_bk_pg->GetData());
  auto new_bk_idx = dir_page->GetSplitImageIndex(bucket_idx);
  dir_page->SetBucketPageId(new_bk_idx, new_bucket_pgid);
  dir_page->SetLocalDepth(new_bk_idx, bucket_depth + 1);
  dir_page->SetLocalDepth(bucket_idx, bucket_depth + 1);

  // insert data into two buckets

  for (const auto &data : old_bucket_data) {
    uint32_t tg_idx = Hash(data.first) & (dir_page->GetLocalDepthMask(new_bk_idx));
    if (tg_idx == new_bk_idx) {
      new_bucket->Insert(data.first, data.second, comparator_);
    } else {
      bucket->Insert(data.first, data.second, comparator_);
    }
  }
  //
  // update links
  size_t distance = 1 << dir_page->GetLocalDepth(new_bk_idx);
  for (size_t i = bucket_idx; i < dir_page->Size(); i += distance) {
    dir_page->SetBucketPageId(i, bucket_idx);
    dir_page->SetLocalDepth(bucket_idx, dir_page->GetLocalDepth(bucket_idx));
  }
  for (size_t i = bucket_idx; i >= distance; i -= distance) {
    dir_page->SetBucketPageId(i, bucket_idx);
    dir_page->SetLocalDepth(bucket_idx, dir_page->GetLocalDepth(bucket_idx));
  }
  for (size_t i = new_bk_idx; i < dir_page->Size(); i += distance) {
    dir_page->SetBucketPageId(i, new_bk_idx);
    dir_page->SetLocalDepth(new_bk_idx, dir_page->GetLocalDepth(new_bk_idx));
  }
  for (size_t i = new_bk_idx; i >= distance; i -= distance) {
    dir_page->SetBucketPageId(i, new_bk_idx);
    dir_page->SetLocalDepth(new_bk_idx, dir_page->GetLocalDepth(new_bk_idx));
  }

  // unpin page

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_pgid, true);
  buffer_pool_manager_->UnpinPage(new_bucket_pgid, true);
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();  // rel
  auto bucket_pgid = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_pgid);

  bool success = bucket->Remove(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_pgid, success);
  if (bucket->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();  // rel

  auto bucket_pgid = KeyToPageId(key, dir_page);
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto image_idx = dir_page->GetSplitImageIndex(bucket_idx);
  // check valid

  if (dir_page->GetLocalDepth(bucket_idx) == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }
  if (dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(image_idx)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }
  // delete bucket
  buffer_pool_manager_->UnpinPage(bucket_pgid, false);
  buffer_pool_manager_->DeletePage(bucket_pgid);

  // update links

  auto image_pgid = dir_page->GetBucketPageId(image_idx);

  dir_page->IncrLocalDepth(bucket_idx);
  dir_page->IncrLocalDepth(image_idx);

  for (size_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == static_cast<page_id_t>(bucket_pgid)) {
      dir_page->SetBucketPageId(i, image_pgid);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_idx));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
