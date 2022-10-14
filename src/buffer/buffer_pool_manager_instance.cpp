//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cassert>
#include <cstring>
#include <mutex>

#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page.h"

using std::lock_guard;
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  lock_guard lock{latch_};
  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return false;
  }
  frame_id_t frame = itr->second;
  auto page = &pages_[frame];
  // assert valid
  assert(page->GetPageId() != INVALID_PAGE_ID);

  disk_manager_->WritePage(page->GetPageId(), page->GetData());

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  lock_guard lock{latch_};
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    auto page = &pages_[i];
    if (page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // find page in free list
  assert(page_table_.size() <= pool_size_);
  lock_guard lock{latch_};
  if (!free_list_.empty()) {
    frame_id_t frame = free_list_.back();
    Page *free_page = &pages_[frame];
    free_list_.pop_back();
    *page_id = AllocatePage();
    free_page->page_id_ = *page_id;
    free_page->pin_count_ = 1;
    free_page->is_dirty_ = false;

    memset(free_page->data_, 0, PAGE_SIZE);
    page_table_[*page_id] = frame;
    return free_page;
  }
  // find from lru
  if (replacer_->Size() > 0) {
    frame_id_t frame;
    replacer_->Victim(&frame);  // assert not false
    Page *free_page = &pages_[frame];
    page_table_.erase(free_page->GetPageId());
    if (free_page->IsDirty()) {
      disk_manager_->WritePage(free_page->GetPageId(), free_page->GetData());
    }
    *page_id = AllocatePage();
    free_page->page_id_ = *page_id;
    free_page->pin_count_ = 1;
    free_page->is_dirty_ = false;

    memset(free_page->data_, 0, PAGE_SIZE);
    page_table_[*page_id] = frame;
    return free_page;
  }

  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  lock_guard lock{latch_};
  assert(page_table_.size() <= pool_size_);
  // search in free list
  auto itr = page_table_.find(page_id);
  if (itr != page_table_.end()) {
    frame_id_t frame = itr->second;
    replacer_->Pin(frame);
    pages_[frame].pin_count_++;
    return &pages_[frame];
  }
  // find from free list
  if (!free_list_.empty()) {
    frame_id_t frame = free_list_.back();
    Page *free_page = &pages_[frame];
    free_list_.pop_back();
    free_page->page_id_ = page_id;
    free_page->pin_count_ = 1;  // TODO(wind):
    free_page->is_dirty_ = false;
    page_table_[page_id] = frame;
    disk_manager_->ReadPage(page_id, free_page->GetData());
    return free_page;
  }
  // find from replacer
  if (replacer_->Size() > 0) {
    frame_id_t frame;
    replacer_->Victim(&frame);  // assert not false
    Page *free_page = &pages_[frame];
    // check dirty
    page_table_.erase(free_page->GetPageId());
    if (free_page->IsDirty()) {
      disk_manager_->WritePage(free_page->GetPageId(), free_page->GetData());
    }

    // change data for new page
    free_page->page_id_ = page_id;
    free_page->pin_count_ = 1;  // TODO(wind):
    free_page->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, free_page->GetData());
    page_table_[page_id] = frame;
    return free_page;
  }

  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  lock_guard lock{latch_};
  assert(page_table_.size() <= pool_size_);

  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = itr->second;
  auto page = &pages_[frame_id];

  if (page->GetPinCount() != 0) {
    return false;
  }

  DeallocatePage(page->GetPageId());
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.emplace_front(frame_id);
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  lock_guard lock{latch_};

  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return true;
  }

  auto *page = &pages_[itr->second];
  if (page->GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(itr->second);
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
