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
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t available_frame_id = -1;
  if (!AvailableFrameJudgement(&available_frame_id)) {
    return nullptr;
  }
  auto new_page = AllocatePage();
  pages_[available_frame_id].ResetMemory();
  pages_[available_frame_id].pin_count_ = 1;
  pages_[available_frame_id].is_dirty_ = false;
  pages_[available_frame_id].page_id_ = new_page;
  replacer_->RecordAccess(available_frame_id);
  replacer_->SetEvictable(available_frame_id, false);

  page_table_->Insert(new_page, available_frame_id);
  *page_id = new_page;
  return &pages_[available_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  auto existing_frame_id = -1;
  if (page_table_->Find(page_id, existing_frame_id)) {
    replacer_->RecordAccess(existing_frame_id);
    replacer_->SetEvictable(existing_frame_id, false);
    pages_[existing_frame_id].pin_count_++;  // pin_count 记录了访问这个页面的线程数量
    return &pages_[existing_frame_id];
  }
  frame_id_t available_frame_id = -1;
  if (!AvailableFrameJudgement(&available_frame_id)) {
    return nullptr;
  }

  pages_[available_frame_id].ResetMemory();
  pages_[available_frame_id].page_id_ = page_id;
  pages_[available_frame_id].pin_count_ = 1;
  pages_[available_frame_id].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[available_frame_id].GetData());

  replacer_->RecordAccess(available_frame_id);
  replacer_->SetEvictable(available_frame_id, false);
  page_table_->Insert(page_id, available_frame_id);
  return &pages_[available_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto unpin_frame = -1;
  if (!page_table_->Find(page_id, unpin_frame)) {
    return false;
  }
  if (pages_[unpin_frame].pin_count_ == 0) {
    return false;
  }
  if (--pages_[unpin_frame].pin_count_ == 0) {
    replacer_->SetEvictable(unpin_frame, true);
  }
  if (is_dirty) {
    pages_[unpin_frame].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto flush_frame = -1;
  if (!page_table_->Find(page_id, flush_frame)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[flush_frame].GetData());
  pages_[flush_frame].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(i, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto delete_frame = -1;
  if (!page_table_->Find(page_id, delete_frame)) {
    return true;
  }
  if (pages_[delete_frame].pin_count_ != 0) {
    return false;
  }
  if (pages_[delete_frame].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[delete_frame].GetData());
    pages_[delete_frame].is_dirty_ = false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(delete_frame);
  free_list_.push_back(delete_frame);
  pages_[delete_frame].ResetMemory();
  pages_[delete_frame].page_id_ = INVALID_PAGE_ID;
  pages_[delete_frame].is_dirty_ = false;
  pages_[delete_frame].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::AvailableFrameJudgement(frame_id_t *available_frame_id) -> bool {
  if (!free_list_.empty()) {
    *available_frame_id = free_list_.back();
    free_list_.pop_back();
    return true;
  }
  if (replacer_->Evict(available_frame_id)) {
    if (pages_[*available_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[*available_frame_id].page_id_, pages_[*available_frame_id].GetData());
      pages_[*available_frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[*available_frame_id].page_id_);
    return true;
  }
  return false;
}

}  // namespace bustub
