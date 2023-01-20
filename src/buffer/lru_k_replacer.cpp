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
#include <unistd.h>
#include <iterator>
#include <mutex>
#include "common/logger.h"
#include "type/limits.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool victim_found = false;
  if (!hist_list_.empty()) {
    for (auto iter = hist_list_.cbegin(); iter != hist_list_.cend(); ++iter) {
      if (records_[*iter].evictable_) {
        *frame_id = *iter;
        hist_list_.erase(iter);
        victim_found = true;
        break;
      }
    }
  }
  if (!victim_found && !cache_list_.empty()) {
    for (auto iter = cache_list_.cbegin(); iter != cache_list_.cend(); ++iter) {
      if (records_[*iter].evictable_) {
        *frame_id = *iter;
        cache_list_.erase(iter);
        victim_found = true;
        break;
      }
    }
  }
  if (victim_found) {
    curr_size_--;
    records_.erase(*frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");
  std::scoped_lock<std::mutex> lock(latch_);
  auto new_count = ++records_[frame_id].hit_count_;
  if (new_count == 1) {
    ++curr_size_;
    hist_list_.push_back(frame_id);
    records_[frame_id].pos_ = std::prev(hist_list_.end());
  } else if (new_count == k_) {
    hist_list_.erase(records_[frame_id].pos_);
    cache_list_.push_back(frame_id);
    records_[frame_id].pos_ = std::prev(cache_list_.end());
  } else if (new_count > k_) {
    cache_list_.erase(records_[frame_id].pos_);
    cache_list_.push_back(frame_id);
    records_[frame_id].pos_ = std::prev(cache_list_.end());
  } else {
    // do nothing
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");
  std::scoped_lock<std::mutex> lock(latch_);
  if (records_.find(frame_id) == records_.end()) {
    return;
  }
  if (set_evictable && !records_[frame_id].evictable_) {
    ++curr_size_;
  } else if (!set_evictable && records_[frame_id].evictable_) {
    --curr_size_;
  }
  records_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (records_.find(frame_id) == records_.end()) {
    return;
  }
  BUSTUB_ASSERT(records_[frame_id].evictable_, "Cannot remove unevictable element");
  if (records_[frame_id].hit_count_ < k_) {
    hist_list_.erase(records_[frame_id].pos_);
  } else {
    cache_list_.erase(records_[frame_id].pos_);
  }
  --curr_size_;
  records_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
