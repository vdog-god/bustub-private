// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // lru_k_replacer.h
// //
// // Identification: src/include/buffer/lru_k_replacer.h
// //
// // Copyright (c) 2015-2022, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// #include <exception>
// #include <limits>
// #include <list>
// #include <mutex>  // NOLINT
// #include <unordered_map>
// #include <vector>
// #include <iterator>

// #include "common/config.h"
// #include "common/macros.h"

// namespace bustub {

// /**
//  * LRUKFrameRecord implements a frame's access statistics in the LRUKReplacer
//  */
// class LRUKRecordVector {
//  private:
//   struct Counter{
//     int32_t freq;
//     bool evic;
//   };
//   size_t lruk_size{0};
//   std::list<frame_id_t> level1_list;
//   std::list<frame_id_t> level2_list;
//   std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> map_1;
//   std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> map_2;
//   std::unordered_map<frame_id_t,Counter>freq_evic_table;//访问次数计数器
//  public:
//   LRUKRecordVector();
//   size_t GetSize();
//   bool InsertFrame(frame_id_t frame_id,size_t k);
//   bool DeleteFrame(frame_id_t frame_id);
//   bool AccessFrame(frame_id_t frame_id,size_t k);//访问次数+1
//   bool Is_2Level(frame_id_t frame_id);
//   bool GetFrame(frame_id_t frame_id);//在hashmap中寻找
//   size_t GetAccessCount(frame_id_t frame_id);
//   bool Setevictable(frame_id_t frame_id,bool et_evictable);
//   bool Getevictable(frame_id_t frame_id);
//   size_t pop();
//   friend class LRUKReplacer;
// };

// /**
//  * LRUKReplacer implements the LRU-k replacement policy.
//  *
//  * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
//  * of all frames. Backward k-distance is computed as the difference in time between
//  * current timestamp and the timestamp of kth previous access.
//  *
//  * A frame with less than k historical references is given
//  * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
//  * classical LRU algorithm is used to choose victim.
//  */
// class LRUKReplacer {
//  public:
//   /**
//    *
//    * TODO(P1): Add implementation
//    *
//    * @brief a new LRUKReplacer.
//    * @param num_frames the maximum number of frames the LRUReplacer will be required to store
//    */
//   explicit LRUKReplacer(size_t num_frames, size_t k);

//   DISALLOW_COPY_AND_MOVE(LRUKReplacer);

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Destroys the LRUReplacer.
//    */
//   ~LRUKReplacer() = default;

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
//    * that are marked as 'evictable' are candidates for eviction.
//    *
//    * A frame with less than k historical references is given +inf as its backward k-distance.
//    * If multiple frames have inf backward k-distance, then evict the frame with the earliest
//    * timestamp overall.
//    *
//    * Successful eviction of a frame should decrement the size of replacer and remove the frame's
//    * access history.
//    *
//    * @param[out] frame_id id of frame that is evicted.
//    * @return true if a frame is evicted successfully, false if no frames can be evicted.
//    */
//   auto Evict(frame_id_t *frame_id) -> bool;

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Record the event that the given frame id is accessed at current timestamp.
//    * Create a new entry for access history if frame id has not been seen before.
//    *
//    * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
//    * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
//    *
//    * @param frame_id id of frame that received a new access.
//    */
//   void RecordAccess(frame_id_t frame_id);

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Toggle whether a frame is evictable or non-evictable. This function also
//    * controls replacer's size. Note that size is equal to number of evictable entries.
//    *
//    * If a frame was previously evictable and is to be set to non-evictable, then size should
//    * decrement. If a frame was previously non-evictable and is to be set to evictable,
//    * then size should increment.
//    *
//    * If frame id is invalid, throw an exception or abort the process.
//    *
//    * For other scenarios, this function should terminate without modifying anything.
//    *
//    * @param frame_id id of frame whose 'evictable' status will be modified
//    * @param set_evictable whether the given frame is evictable or not
//    */
//   void SetEvictable(frame_id_t frame_id, bool set_evictable);

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Remove an evictable frame from replacer, along with its access history.
//    * This function should also decrement replacer's size if removal is successful.
//    *
//    * Note that this is different from evicting a frame, which always remove the frame
//    * with largest backward k-distance. This function removes specified frame id,
//    * no matter what its backward k-distance is.
//    *
//    * If Remove is called on a non-evictable frame, throw an exception or abort the
//    * process.
//    *
//    * If specified frame is not found, directly return from this function.
//    *
//    * @param frame_id id of frame to be removed
//    */
//   void Remove(frame_id_t frame_id);

//   /**
//    * TODO(P1): Add implementation
//    *
//    * @brief Return replacer's size, which tracks the number of evictable frames.
//    *
//    * @return size_t
//    */
//   auto Size() -> size_t;

//  private:
//   // TODO(student): implement me! You can replace these member variables as you like.
//   // Remove maybe_unused if you start using them.
//   [[maybe_unused]] size_t current_timestamp_{0};
//   [[maybe_unused]] size_t curr_size_{0};
//   size_t replacer_size_;
//   size_t k;
//   // size_t num_frames;
//   std::mutex latch_;
//   LRUKRecordVector carrier;
// };

// }  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

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
   */
  void RecordAccess(frame_id_t frame_id);

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
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

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
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  void DeleteFrame(frame_id_t frame_id);

  auto GetFrameEvict(frame_id_t frame_id) -> bool;

  auto GetFrame(frame_id_t frame_id) -> bool;

  // auto CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  size_t max_size_;

  using timestamp = std::list<size_t>;  // 记录单个页时间戳的列表
  using k_time = std::pair<frame_id_t, size_t>;
  std::unordered_map<frame_id_t, timestamp> hist_;       // 用于记录所有页的时间戳
  std::unordered_map<frame_id_t, size_t> recorded_cnt_;  // 用于记录,访问了多少次
  std::unordered_map<frame_id_t, bool> evictable_;       // 用于记录是否可以被驱逐

  std::list<frame_id_t> new_frame_;  // 用于记录不满k次的页
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> new_locate_;

  std::list<k_time> cache_frame_;  // 用于记录到达k次的页
  std::unordered_map<frame_id_t, std::list<k_time>::iterator> cache_locate_;
  static auto CmpTimestamp(const k_time &f1, const k_time &f2) -> bool;

  // // map是非线程安全的
  // std::unordered_map<frame_id_t, size_t> access_frequence_count_;//用于记录访问频度
  // std::unordered_map<frame_id_t, bool> is_evictable_;//用于记录是否可驱逐

  // std::list<frame_id_t> level1_list_;//用于记录不满k次的页
  // std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> level1_map_;

  // std::list<frame_id_t> level2_list_;//用于记录达到k次的页
  // std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> level2_map_;
};

}  // namespace bustub
