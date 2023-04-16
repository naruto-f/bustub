//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;
  page_id_t origin_page_id = INVALID_PAGE_ID;

  std::lock_guard<std::mutex> lock(latch_);
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    origin_page_id = pages_[frame_id].page_id_;
  } else {
    return nullptr;
  }

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  *page_id = AllocatePage();
  if (origin_page_id != INVALID_PAGE_ID) {
    page_table_.erase(origin_page_id);
  }
  page_table_[*page_id] = frame_id;
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;

  if (origin_page_id != INVALID_PAGE_ID) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(origin_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
  }

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id = -1;
  page_id_t origin_page_id = INVALID_PAGE_ID;

  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) != 0) {
    ++pages_[page_table_[page_id]].pin_count_;
    replacer_->SetEvictable(page_table_[page_id], false);
    replacer_->RecordAccess(page_table_[page_id]);
    return &pages_[page_table_[page_id]];
  }

  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    origin_page_id = pages_[frame_id].page_id_;
  } else {
    return nullptr;
  }

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  if (pages_[frame_id].GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(origin_page_id);
  }
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;

  if (origin_page_id != INVALID_PAGE_ID) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(origin_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
  }

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) == 0 || pages_[page_table_[page_id]].pin_count_ == 0) {
    return false;
  }

  Page *target_page = &pages_[page_table_[page_id]];

  if (--target_page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  if (is_dirty) {
    target_page->is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  Page *target_page = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(page_id, target_page->data_);
  target_page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &[page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *target_page = &pages_[frame_id];
  if (target_page->pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  target_page->page_id_ = INVALID_PAGE_ID;
  target_page->pin_count_ = 0;
  target_page->is_dirty_ = false;
  target_page->ResetMemory();

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    return {this, page};
  }
  return {};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
    return {this, page};
  }
  return {};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
    return {this, page};
  }
  return {};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page != nullptr) {
    return {this, page};
  }
  return {};
}

}  // namespace bustub
