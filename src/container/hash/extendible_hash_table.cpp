
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];

    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      for (int i = 0; i < capacity; i++) {
        dir_[i + capacity] = dir_[i];
      }
    }

    int mask = 1 << target_bucket->GetDepth();
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);

    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }

    num_buckets_++;

    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  for (auto &item : target_bucket->GetItems()) {
    if (item.first == key) {
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &item) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

//===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // extendible_hash_table.cpp
// //
// // Identification: src/container/hash/extendible_hash_table.cpp
// //
// // Copyright (c) 2022, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <cassert>
// #include <cstdlib>
// #include <functional>
// #include <list>
// #include <utility>

// #include "container/hash/extendible_hash_table.h"
// #include "storage/page/page.h"

// namespace bustub {

// template <typename K, typename V>
// ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
//     : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
//   dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
//   int mask = (1 << global_depth_) - 1;
//   return std::hash<K>()(key) & mask;
//   //
//   Q:hash是怎么把key映射成目录索引的？A:取余和相与之间存在关系1个数和5进行与运算就是对6取余,和7进行与运算就是对8取余,依次类推。这种把取余操作转化为按位与操作,只能适用于2^n。
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return GetGlobalDepthInternal();
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
//   return global_depth_;
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return GetLocalDepthInternal(dir_index);
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
//   return dir_[dir_index]->GetDepth();
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return GetNumBucketsInternal();
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
//   return num_buckets_;
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return dir_[IndexOf(key)]->Find(key, value);
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
//   std::scoped_lock<std::mutex> lock(latch_);
//   return dir_[IndexOf(key)]->Remove(key);
// }

// template <typename K, typename V>
// void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
//   std::scoped_lock<std::mutex> lock(latch_);

//   while (dir_[IndexOf(key)]->IsFull()) {
//     auto index = IndexOf(key);
//     auto target_bucket = dir_[index];

//     if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
//       global_depth_++;
//       int capacity = dir_.size();
//       dir_.resize(capacity << 1);
//       for (int i = 0; i < capacity; i++) {
//         dir_[i + capacity] = dir_[i];
//       }
//     }

//     int mask = 1 << target_bucket->GetDepth();
//     auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
//     auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);

//     for (const auto &item : target_bucket->GetItems()) {
//       size_t hash_key = std::hash<K>()(item.first);
//       if ((hash_key & mask) != 0U) {
//         bucket_1->Insert(item.first, item.second);
//       } else {
//         bucket_0->Insert(item.first, item.second);
//       }
//     }

//     num_buckets_++;

//     for (size_t i = 0; i < dir_.size(); i++) {
//       if (dir_[i] == target_bucket) {
//         if ((i & mask) != 0U) {
//           dir_[i] = bucket_1;
//         } else {
//           dir_[i] = bucket_0;
//         }
//       }
//     }
//   }

//   auto index = IndexOf(key);
//   auto target_bucket = dir_[index];

//   for (auto &item : target_bucket->GetItems()) {
//     if (item.first == key) {
//       item.second = value;
//       return;
//     }
//   }

//   target_bucket->Insert(key, value);
//   //以下为自己编写bug部分。bug在目录的重新索引和分配部分，不知道为什么会出bug
//   // //先检查是否是以存在的key，已存在则更新value
//   //   auto index = IndexOf(key);
//   //   auto target_bucket = dir_[index];
//   // for (auto &temp : target_bucket->GetItems()) {
//   //   if (temp.first == key) {
//   //     temp.second = value;
//   //     return ;
//   //   }
//   // }

//   // while (dir_[IndexOf(key)]->IsFull()) {
//   //   auto index = IndexOf(key);
//   //   auto target_bucket = dir_[index];
//   //   auto dir_size =std::pow(2,global_depth_);                  // or dir_.size()
//   //   if (target_bucket->GetDepth() == GetGlobalDepthInternal())  // 进行目录扩增
//   //   {
//   //     global_depth_++;
//   //     dir_.resize(dir_size * 2);
//   //     for (int i = 0; i < dir_size; i++) {
//   //       dir_[i + dir_size] = dir_[i];
//   //     }
//   //   }

//   //   // 深度+1，遍历元素是否和当前索引号相同，找出不同的放在新分裂的桶中，同时新分裂的桶需要放在合适的目录
//   //   auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
//   //   auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
//   //   // 桶分裂后，元素重新分配
//   //   for (const auto &temp : target_bucket->GetItems()) {
//   //     if ((IndexOf(temp.first) >> (global_depth_)) == 1U) {
//   //       bucket_1->Insert(temp.first, temp.second);
//   //     } else {
//   //       bucket_0->Insert(temp.first, temp.second);
//   //     }
//   //   }
//   //   num_buckets_++;
//   //   // 重新分配目录指针
//   //   for (size_t i = 0; i < dir_.size(); i++) {
//   //     // 后localdepth的位数相同的目录指向同一个buckect
//   //     if (dir_[i] == target_bucket) {
//   //       if ((i >> global_depth_) == 1U) {
//   //         dir_[i] = bucket_1;  // 智能指针再指向另一个地址后，原地址的计数器会自动-1
//   //       } else {
//   //         dir_[i] = bucket_0;
//   //       }
//   //     }
//   //   }
//   //   }
//   // target_bucket->Insert(key, value);
// }

// //===--------------------------------------------------------------------===//
// // Bucket
// //===--------------------------------------------------------------------===//
// template <typename K, typename V>
// ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) {
//   size_ = array_size;
//   depth_ = depth;
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
//   for (auto temp = list_.begin(); temp != list_.end(); temp++) {
//     if ((*temp).first == key) {
//       value = (*temp).second;
//       return true;
//     }
//   }
//   return false;
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
//   for (auto temp = list_.begin(); temp != list_.end(); temp++) {
//     if ((*temp).first == key) {
//       list_.erase(temp);
//       return true;
//     }
//   }
//   return false;
// }

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
//   if (this->IsFull()) {
//     return false;
//   }

//   // for (auto &temp : list_) {
//   //   if (temp.first == key) {
//   //     temp.second = value;
//   //     return ;
//   //   }
//   // }
//   list_.emplace_back(key, value);
//   return true;
// }

// template class ExtendibleHashTable<page_id_t, Page *>;
// template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
// template class ExtendibleHashTable<int, int>;
// // test purpose
// template class ExtendibleHashTable<int, std::string>;
// template class ExtendibleHashTable<int, std::list<int>::iterator>;

// }  // namespace bustub
