// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // lru_k_replacer.cpp
// //
// // Identification: src/buffer/lru_k_replacer.cpp
// //
// // Copyright (c) 2015-2022, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "buffer/lru_k_replacer.h"
// #include <exception>
// #include <iterator>

// namespace bustub {
// LRUKRecordVector::LRUKRecordVector() {}

// bool LRUKRecordVector::AccessFrame(frame_id_t frame_id, size_t k) {
// if (GetFrame(frame_id)) {
//     freq_evic_table[frame_id].freq++;
//     DeleteFrame(frame_id);
//   } else {
//     freq_evic_table[frame_id].freq = 1;
//   }
//   InsertFrame(frame_id, k);
//   return true;
// }

// bool LRUKRecordVector::DeleteFrame(frame_id_t frame_id) {
//   if (!GetFrame(frame_id)) {
//     return false;
//   }
//   if (Is_2Level(frame_id)) {
//   auto it=map_2[frame_id];
//     level2_list.erase(it);
//     map_2.erase(frame_id);
//   } else {
//     const auto& it=map_1[frame_id];
//     level1_list.erase(it);
//     map_1.emplace(frame_id);
//     map_1.erase(frame_id);//在删除list中frame_id的迭代器后，map中指向该元素的迭代器失效。再次erase会出现问题
//   }
//   freq_evic_table.erase(frame_id);
//   return true;
// }

// size_t LRUKRecordVector::pop() {
//   for (auto ptr = level1_list.begin(); ptr != level1_list.end(); ptr++) {
//     bool condition = freq_evic_table[(*ptr).frame_id].evic;
//     if (condition) {
//       size_t frame_pop_id = (*ptr).frame_id;
//       map_1.erase((*ptr).frame_id);
//       level1_list.erase(ptr);
//       freq_evic_table.erase(frame_pop_id);
//       lruk_size--;
//       return frame_pop_id;
//     }
//   }
//   for (auto ptr = (level2_list.begin()); ptr != level2_list.end(); ptr++) {//错误的写法
//     bool condition = freq_evic_table[(*ptr)].evic;
//     if (condition) {
//       size_t frame_pop_id = (*ptr);
//       map_2.erase(frame_pop_id);
//       level2_list.erase(ptr);
//       freq_evic_table.erase(frame_pop_id);
//       lruk_size--;
//       return frame_pop_id;
//     }
//   }
//   return 0;
// }

// size_t LRUKRecordVector::GetSize() { return (map_1.size() + map_2.size()); }

// bool LRUKRecordVector::Setevictable(frame_id_t frame_id, bool set_evictable) {
//   if (GetFrame(frame_id)) {
//     lruk_size++;
//     if (!set_evictable) {
//       lruk_size--;
//     }
//     freq_evic_table[frame_id].evic = set_evictable;
//     return true;
//   }
//   return false;
// }

// bool LRUKRecordVector::Getevictable(frame_id_t frame_id) { return freq_evic_table[frame_id].evic; }
// size_t LRUKRecordVector::GetAccessCount(frame_id_t frame_id) { return freq_evic_table[frame_id].freq; }

// bool LRUKRecordVector::InsertFrame(frame_id_t frame_id, size_t k) {
//   if(GetAccessCount(frame_id) > (k - 1)) {
//     map_2[frame_id] = (level2_list.begin());
//     //!!标准库函数的list数据结构中，一旦list建立，end()函数的地址就是固定的，无论向list中push_back()还是erase()。可以用rbegin()
//     level2_list.push_front(frame_id);
//   } else {
//     map_1[frame_id] = level1_list.begin();
//     level1_list.push_front(frame_id);
//   }
//   return true;
// }

// bool LRUKRecordVector::GetFrame(frame_id_t frame_id) {
//   if (map_2.count(frame_id)||map_1.count(frame_id)) {
//     return true;
//   }
//     return false;

// }

// bool LRUKRecordVector::Is_2Level(frame_id_t frame_id) {
//   if (map_2.count(frame_id) == 1) {
//     return true;
//   }
//   return false;
// }

// LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k(k) { LRUKRecordVector
// carrier; }

// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (carrier.pop() == 0) {
//     return false;
//   }
//   *frame_id = carrier.pop();
//   return true;
// }

// void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
//   std::scoped_lock<std::mutex> lock(latch_);

//   if ((carrier.GetSize() == replacer_size_) && !carrier.GetFrame(frame_id)) {
//     throw std::exception();
//   }
//   carrier.AccessFrame(frame_id, k);
// }

// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//   //
//   此处本来为了让replace和FrameVector分工明确，想用友元函数实现，但不知道为何编译器无法识别私有成员，故先在vector中实现
//   carrier.Setevictable(frame_id, set_evictable);
// }

// void LRUKReplacer::Remove(frame_id_t frame_id) {
//   if (!carrier.GetFrame(frame_id)) {
//     return;
//   }
//   if (!carrier.Getevictable(frame_id)) {
//     throw std::exception();
//   }
//     carrier.DeleteFrame(frame_id);
//     carrier.lruk_size--;
//   }

// auto LRUKReplacer::Size() -> size_t { return carrier.lruk_size; }

// }  // namespace bustub
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
#include <exception>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  /**
   * 如果没有可以驱逐元素
   */
  if (Size() == 0) {
    return false;
  }
  /**
   * 首先尝试删除距离为无限大的缓存
   */
  for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++) {
    auto frame = *it;
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      new_locate_.erase(frame);
      new_frame_.remove(frame);
      *frame_id = frame;
      curr_size_--;
      hist_[frame].clear();
      return true;
    }
  }

  /**
   * 再尝试删除已经访问过K次的缓存
   */
  for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++) {
    auto frame = (*it).first;
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      cache_frame_.erase(it);
      cache_locate_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      hist_[frame].clear();
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  current_timestamp_++;
  recorded_cnt_[frame_id]++;
  auto cnt = recorded_cnt_[frame_id];
  hist_[frame_id].push_back(current_timestamp_);
  /**
   * 如果是新加入的记录
   */
  if (cnt == 1) {
    if (curr_size_ == max_size_) {
      // 将evict主体改造
      /**
       * 先尝试删除距离无限大的缓存
       */
      auto result = false;
      for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++) {
        auto frame_e = *it;
        if (evictable_[frame_e]) {
          recorded_cnt_[frame_e] = 0;
          new_locate_.erase(frame_e);
          new_frame_.remove(frame_e);
          curr_size_--;
          hist_[frame_e].clear();
          result = true;
          break;
        }
      }
      if (!result) {
        /**
         * 若删除失败再尝试删除已经访问过K次的缓存
         */
        for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++) {
          auto frame_e = (*it).first;
          if (evictable_[frame_e]) {
            recorded_cnt_[frame_e] = 0;
            cache_frame_.erase(it);
            cache_locate_.erase(frame_e);
            curr_size_--;
            hist_[frame_e].clear();
            break;
          }
        }
      }
    }
    evictable_[frame_id] = true;
    curr_size_++;
    new_frame_.push_front(frame_id);
    new_locate_[frame_id] = new_frame_.begin();
  }
  /**
   * 如果记录达到k次，则需要从新队列中加入到老队列中
   */
  if (cnt == k_) {
    new_frame_.erase(new_locate_[frame_id]);  // 从新队列中删除
    new_locate_.erase(frame_id);

    auto kth_time = hist_[frame_id].front();  // 获取当前页面的倒数第k次出现的时间
    k_time new_cache(frame_id, kth_time);
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);  // 找到该插入的位置
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
  /**
   * 如果记录在k次以上，需要将该frame放到指定的位置
   */
  if (cnt > k_) {
    hist_[frame_id].erase(hist_[frame_id].begin());
    cache_frame_.erase(cache_locate_[frame_id]);  // 去除原来的位置
    auto kth_time = hist_[frame_id].front();      // 获取当前页面的倒数第k次出现的时间
    k_time new_cache(frame_id, kth_time);

    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);  // 找到该插入的位置
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
  /**
   * 如果cnt<k_，是不需要做更新动作的
   */
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (recorded_cnt_[frame_id] == 0) {
    return;
  }
  auto status = evictable_[frame_id];
  evictable_[frame_id] = set_evictable;
  if (status && !set_evictable) {
    --max_size_;
    --curr_size_;
  }
  if (!status && set_evictable) {
    ++max_size_;
    ++curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  auto cnt = recorded_cnt_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  } else {
    cache_frame_.erase(cache_locate_[frame_id]);
    cache_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool {
  return f1.second < f2.second;
}
}  // namespace bustub
// void LRUKReplacer::DeleteFrame(frame_id_t frame_id) {  // delete之前需要判断能不能delete及一些列删除的准备和善后工作
//   if (level1_map_.count(frame_id) == 1) {
//     auto it = *level1_map_[frame_id];
//     level1_list_.erase(level1_map_[frame_id]);
//     level1_map_.erase(it);  // unorder_map[i]返回的是引用类型
//   } else if (level2_map_.count(frame_id) == 1) {
//     auto it = *level2_map_[frame_id];
//     level2_list_.erase(level2_map_[frame_id]);
//     level2_map_.erase(it);
//   }
// }
// auto LRUKReplacer::GetFrameEvict(frame_id_t frame_id) -> bool {
//   // 多线程下需要加锁否则不安全
//   return is_evictable_.at(frame_id);
// }
// auto LRUKReplacer::GetFrame(frame_id_t frame_id) -> bool { return (access_frequence_count_.count(frame_id) >= 1); }

// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
//   // if(replacer is empty){
//   //   return flase
//   // }
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (curr_size_ == 0) {
//     return false;
//   }

//   // for(遍历level1 ){
//   //   if(evit is true){
//   //   delete frame
//   //   return true
//   //   }
//   //   ++
//   // }
//   // for (遍历level2) {
//   //   if (evit is true) {
//   //     delete frame return true
//   //   }
//   //   ++
//   // }
//   // return false;
//   for (auto itptr = level1_list_.rbegin(); itptr != level1_list_.rend(); itptr++) {
//     auto frame = *itptr;
//     if (is_evictable_.at(frame)) {
//       *frame_id = frame;
//       access_frequence_count_.erase(frame);
//       is_evictable_.erase(frame);
//       // 出于多线程需要 ，现在解耦合，将deleteFrame接口替换。类似的还有GetFrame和GetFrameEvict

//       // DeleteFrame(frame);
//       if (level1_map_.count(frame) == 1) {
//         auto it = *level1_map_[frame];
//         level1_list_.erase(level1_map_[frame]);
//         level1_map_.erase(it);  // unorder_map[i]返回的是引用类型
//       } else if (level2_map_.count(frame) == 1) {
//         auto it = *level2_map_[frame];
//         level2_list_.erase(level2_map_[frame]);
//         level2_map_.erase(it);
//       }
//       curr_size_--;
//       return true;
//     }
//   }
//   for (auto itptr = level2_list_.rbegin(); itptr != level2_list_.rend(); itptr++) {
//     auto frame = *itptr;
//     if (is_evictable_.at(frame)) {
//       *frame_id = frame;
//       access_frequence_count_.erase(frame);
//       is_evictable_.erase(frame);
//       // DeleteFrame(frame);
//       if (level1_map_.count(frame) == 1) {
//         auto it = *level1_map_[frame];
//         level1_list_.erase(level1_map_[frame]);
//         level1_map_.erase(it);  // unorder_map[i]返回的是引用类型
//       } else if (level2_map_.count(frame) == 1) {
//         auto it = *level2_map_[frame];
//         level2_list_.erase(level2_map_[frame]);
//         level2_map_.erase(it);
//       }
//       curr_size_--;
//       return true;
//     }
//   }
//   return false;
// }

// void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
//   //   if (invalid) {
//   //     throw std::exception();
//   //   }
//   //   if (not access accord) {
//   //     if (level1_list is full) {
//   //       select outdate frame to evict
//   //     }
//   //     build a new entry frequence + 1
//   //   } else {
//   //     current - frame frequence + 1 delete frame of level1list if (frame frequence counter >= k) { join level2list
//   }
//   //     else {
//   //       rejoin level1list
//   //     }
//   //   }
//   // }
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (frame_id > static_cast<int>(replacer_size_)) {
//     LOG_DEBUG("frame_id %d>replacer_size_%d", frame_id, static_cast<int>(replacer_size_));
//     throw std::exception();
//   }
//   access_frequence_count_[frame_id]++;  //
//   if (access_frequence_count_[frame_id] == k_) {
//     auto it = *level1_map_[frame_id];
//     level1_list_.erase(level1_map_[frame_id]);
//     level1_map_.erase(it);  // unorder_map[i]返回的是引用类型
//     level2_list_.push_front(frame_id);
//     level2_map_[frame_id] = level2_list_.begin();
//   } else if (access_frequence_count_[frame_id] > k_) {
//     if (level2_map_.count(frame_id) != 0) {
//       level2_list_.erase(level2_map_[frame_id]);
//     }
//     level2_list_.push_front(frame_id);
//     level2_map_[frame_id] = level2_list_.begin();
//   } else {
//     level1_list_.push_front(frame_id);
//     level1_map_[frame_id] = level1_list_.begin();
//   }
// }

// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//   // if (invalid) {
//   //   throw std::exception();
//   // }
//   // set true or false if true, size + 1.if false, size - 1
//   std::scoped_lock<std::mutex> lock(latch_);

//   if (frame_id > static_cast<int>(replacer_size_)) {
//     LOG_DEBUG("frame_id %d>replacer_size_%d", frame_id, static_cast<int>(replacer_size_));
//     throw std::exception();
//   }
//   if (!(access_frequence_count_.count(frame_id) >= 1)) {
//     return;
//   }

//   if (!is_evictable_[frame_id] && set_evictable) {
//     curr_size_++;
//   }
//   if (is_evictable_[frame_id] && !set_evictable) {
//     curr_size_--;
//   }
//   is_evictable_[frame_id] = set_evictable;
// }

// void LRUKReplacer::Remove(frame_id_t frame_id) {
//   // if (is not evict) {
//   //   throw std::exception()
//   // }
//   // delete frame from level1 or level2 size - 1
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (frame_id > static_cast<int>(replacer_size_)) {
//     LOG_DEBUG("frame_id %d>replacer_size_%d", frame_id, static_cast<int>(replacer_size_));
//     throw std::exception();
//   }
//   if (!(access_frequence_count_.count(frame_id) >= 1)) {
//     return;
//   }
//   if (!is_evictable_[frame_id]) {
//     LOG_DEBUG("is_evictable_[%d] is false !!", frame_id);
//     throw std::exception();
//   }
//   if (access_frequence_count_[frame_id] < k_) {
//     level1_list_.erase(level1_map_[frame_id]);
//     level1_map_.erase(frame_id);
//   } else {
//     level2_list_.erase(level2_map_[frame_id]);
//     level2_map_.erase(frame_id);
//   }
//   curr_size_--;
//   access_frequence_count_[frame_id] = 0;
//   is_evictable_[frame_id] = false;
// }

// auto LRUKReplacer::Size() -> size_t {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return curr_size_;
// }

// }  // namespace bustub
// 以下为程序逻辑部分
//  namespace bustub {

// LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
// if(replacer is empty){
//   return flase
// }
// for(遍历level1 ){
//   if(evit is true){
//   delete frame
//   return true
//   }
//   ++
// }
// for(遍历level2 ){
//   if(evit is true){
//   delete frame
//   return true
//   }
//   ++
// }
//   return false; }

// void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
//   if(invalid){
//     throw std::exception();
//   }
//   if(not access accord){
//     if(level1_list is full)
//     {
//      select outdate frame to evict
//     }
//      build a new entry
//      frequence+1
//   }else{
//     current-frame frequence+1
//     delete frame of level1list
//     if(frame frequence counter>=k){
//      join level2list
//     }else{
//      rejoin level1list
//     }
//   }
// }

// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//    if(invalid){
//     throw std::exception();
//   }
//   set true or false
//   if true ,size+1.if false ,size-1
// }

// void LRUKReplacer::Remove(frame_id_t frame_id) {
//   if(invalid){
//     return
//   }
//   if(is not evict){
//     throw std::exception()
//   }
//   delete frame from level1 or level2
//   size-1
// }

// auto LRUKReplacer::Size() -> size_t {
//   return size; }

// }  // namespace bustub
