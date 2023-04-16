//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames),
      k_(k),
      less_k_head_(std::make_unique<LRUKNode>(-1, 0)),
      less_k_tail_(std::make_unique<LRUKNode>(-1, 0)) {
  less_k_head_->next_ = less_k_tail_.get();
  less_k_tail_->prev_ = less_k_head_.get();

  for (size_t i = 0; i < num_frames; ++i) {
    std::shared_ptr<LRUKNode> node = std::make_shared<LRUKNode>(i, k_);
    node_store_[i] = node;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (less_k_head_->next_ != less_k_tail_.get()) {
    *frame_id = RemoveEarliestFrameFromLruList();
    --curr_size_;
    return true;
  }

  if (!equal_k_map_.empty()) {
    *frame_id = RemoveEarliestFrameFromMap();
    --curr_size_;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_ && static_cast<size_t>(frame_id) >= 0,
                "The frame id is invalid!");

  std::lock_guard<std::mutex> lock(latch_);

  auto target_node = node_store_[frame_id];
  auto record_size = target_node->history_.size();

  /// 注意:
  /// 这里的时间戳不能用系统时间戳例如linux中的time函数，要使用自定义的时间戳，因为time只能精确到秒，
  /// 其他函数也最多精确到微秒
  /// 而整个测试程序可能就执行不到1微秒，会出现不同时间访问的时间戳值相同，这会影响到LRU-K算法的判断。
  target_node->history_.push_front(current_timestamp_++);
  if (record_size < k_) {
    if (target_node->is_evictable_) {
      if (record_size == k_ - 1) {
        RemoveFromLruList(frame_id);
        InsertToMap(frame_id);
      } else {
        RemoveFromLruList(frame_id);
        InsertToLruList(frame_id);
      }
    }
  } else {
    if (target_node->is_evictable_) {
      RemoveFromMap(frame_id);
      target_node->history_.pop_back();
      InsertToMap(frame_id);
    } else {
      target_node->history_.pop_back();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_ && static_cast<size_t>(frame_id) >= 0,
                "The frame id is invalid!");

  std::lock_guard<std::mutex> lock(latch_);

  auto target_node = node_store_[frame_id];
  if (target_node->is_evictable_ != set_evictable) {
    if (set_evictable) {
      if (target_node->history_.size() < k_) {
        InsertToLruList(frame_id);
      } else {
        InsertToMap(frame_id);
      }
      ++curr_size_;
    } else {
      if (target_node->history_.size() < k_) {
        RemoveFromLruList(frame_id);
      } else {
        RemoveFromMap(frame_id);
      }
      --curr_size_;
    }

    target_node->is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > replacer_size_ || frame_id < 0 || !node_store_[frame_id]->is_evictable_) {
    return;
  }

  std::lock_guard<std::mutex> lock(latch_);

  auto remove_node = node_store_[frame_id];
  // BUSTUB_ASSERT(remove_node->is_evictable_, "Can't remove a non-evictable frame!");

  if (remove_node->history_.size() < k_) {
    RemoveFromLruList(frame_id);
  } else {
    RemoveFromMap(frame_id);
  }
  remove_node->history_.clear();
  remove_node->is_evictable_ = false;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

void LRUKReplacer::InsertToLruList(frame_id_t frame_id) {
  auto node = node_store_[frame_id];
  node->next_ = less_k_tail_.get();
  node->prev_ = less_k_tail_->prev_;
  less_k_tail_->prev_->next_ = node.get();
  less_k_tail_->prev_ = node.get();
}

void LRUKReplacer::RemoveFromLruList(frame_id_t frame_id) {
  auto node = node_store_[frame_id];
  node->next_->prev_ = node->prev_;
  node->prev_->next_ = node->next_;
  node->next_ = nullptr;
  node->prev_ = nullptr;
}

void LRUKReplacer::InsertToMap(frame_id_t frame_id) { equal_k_map_.insert(node_store_[frame_id].get()); }

void LRUKReplacer::RemoveFromMap(frame_id_t frame_id) { equal_k_map_.erase(node_store_[frame_id].get()); }

auto LRUKReplacer::RemoveEarliestFrameFromLruList() -> frame_id_t {
  auto need_evict_frame = less_k_head_->next_;
  less_k_head_->next_ = need_evict_frame->next_;
  need_evict_frame->next_->prev_ = need_evict_frame->prev_;
  need_evict_frame->next_ = nullptr;
  need_evict_frame->prev_ = nullptr;
  need_evict_frame->is_evictable_ = false;
  need_evict_frame->history_.clear();
  return need_evict_frame->fid_;
}

auto LRUKReplacer::RemoveEarliestFrameFromMap() -> frame_id_t {
  frame_id_t fid = (*equal_k_map_.begin())->fid_;
  (*equal_k_map_.begin())->is_evictable_ = false;
  (*equal_k_map_.begin())->history_.clear();
  equal_k_map_.erase(*equal_k_map_.begin());

  return fid;
}

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : fid_(fid), k_(k) {}
}  // namespace bustub
