//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
#include <iostream>
#include <mutex>

using std::lock_guard;
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard lock{lock_};
  if (lru_.empty()) {
    return false;
  }
  *frame_id = lru_.back();
  lru_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard lock{lock_};
  lru_.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard lock{lock_};
  if (lru_.size() >= max_num_) {
  }
  auto itr = std::find(lru_.begin(), lru_.end(), frame_id);
  if (itr != lru_.end()) {
    return;
  }
  lru_.emplace_front(frame_id);
}

size_t LRUReplacer::Size() {
  lock_guard lock{lock_};
  return lru_.size();
}

}  // namespace bustub
