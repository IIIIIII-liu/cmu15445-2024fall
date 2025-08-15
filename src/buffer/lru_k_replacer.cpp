//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <queue>
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // std::lock_guard<std::mutex> lock(latch_);
  // auto cmp = [this](frame_id_t a, frame_id_t b) {
  //   LRUKNode &node_a = node_store_[a];
  //   LRUKNode &node_b = node_store_[b];
  //   if (CalculateBackwardKDistance(node_a) != CalculateBackwardKDistance(node_b)) {
  //     return CalculateBackwardKDistance(node_a) < CalculateBackwardKDistance(node_b);
  //   }
  //   return node_a.history_.front() > node_b.history_.front();
  // };
  // std::priority_queue<frame_id_t, std::vector<frame_id_t>, decltype(cmp)> pq(cmp);
  // for (const auto &[frame_id, node] : node_store_) {
  //   if (node.is_evictable_) {
  //     pq.push(frame_id);
  //   }
  // }
  // if (!pq.empty()) {
  //   frame_id_t victim_frame = pq.top();  // 队首元素是优先级最高的帧
  //   node_store_.erase(victim_frame);
  //   curr_size_--;
  //   return victim_frame;
  // }
  // return std::nullopt;  // 如果没有可淘汰的帧，返回std::nullopt
  std::lock_guard<std::mutex> lk(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  bool have_inf = false;
  frame_id_t best_inf = -1;
  size_t best_inf_oldest = std::numeric_limits<size_t>::max();
  bool have_finite = false;
  frame_id_t best_finite = -1;
  size_t best_finite_dist = 0;
  size_t best_finite_oldest = std::numeric_limits<size_t>::max();

  for (auto &p : node_store_) {
    frame_id_t fid = p.second.fid_;
    auto &node = p.second;
    if (!node.is_evictable_) {
      continue;
    }
    if (node.history_.empty()) {
      continue;  // skip or treat as fallback
    }
    if (node.history_.size() < k_) {
      size_t oldest = node.history_.front();
      if (!have_inf || oldest < best_inf_oldest) {
        have_inf = true;
        best_inf = fid;
        best_inf_oldest = oldest;
      }
    } else {
      size_t kth_time = node.history_.front();
      size_t dist = 0;
      if (current_timestamp_ >= kth_time) {
        dist = current_timestamp_ - kth_time;
      } else {
        dist = 0;
      }
      if (!have_finite || dist > best_finite_dist || (dist == best_finite_dist && kth_time < best_finite_oldest)) {
        have_finite = true;
        best_finite = fid;
        best_finite_dist = dist;
        best_finite_oldest = kth_time;
      }
    }
  }

  frame_id_t victim = -1;
  if (have_inf) {
    victim = best_inf;
  } else if (have_finite) {
    victim = best_finite;
  } else {
    return std::nullopt;
  }

  node_store_.erase(victim);
  if (curr_size_ > 0) {
    --curr_size_;
  }
  return victim;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Invalid frame id");
  }

  std::lock_guard<std::mutex> lk(latch_);
  ++current_timestamp_;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode new_node(k_, frame_id);
    new_node.history_.push_back(current_timestamp_);
    node_store_[frame_id] = new_node;
  } else {
    LRUKNode &node = it->second;
    if (node.history_.size() >= node.k_) {
      node.history_.pop_front();
    }
    node.history_.push_back(current_timestamp_);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Invalid frame id");
  }

  std::lock_guard<std::mutex> lk(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  LRUKNode &node = it->second;
  if (node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    if (set_evictable) {
      ++curr_size_;
    } else if (curr_size_ > 0) {
      --curr_size_;
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Invalid frame id");
  }

  std::lock_guard<std::mutex> lk(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  LRUKNode &node = it->second;
  if (!node.is_evictable_) {
    throw Exception("Frame is not evictable");
  }
  node_store_.erase(it);
  if (curr_size_ > 0) {
    --curr_size_;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CalculateBackwardKDistance(const LRUKNode &node) -> size_t {
  if (node.history_.size() < k_) {
    return std::numeric_limits<size_t>::max();  // +inf
  }
  return current_timestamp_ - node.history_[node.history_.size() - k_];
}

}  // namespace bustub
