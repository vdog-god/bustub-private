//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/type.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchPosition(const KeyType &key, KeyComparator &comparator) const -> int {
  auto low = 0;
  auto high = GetSize() - 1;
  auto bigger_equal_idx = -1;  // bigger是第一个 >= key的位置
  while (low <= high) {
    auto mid = low + (high - low) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return mid;
    }
    if (comp_res < 0) {
      bigger_equal_idx = mid;
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return bigger_equal_idx;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyType &key, std::vector<ValueType> *search_result,
                                              KeyComparator &comparator) const -> bool {
  // leafPage直接返回value
  int low = 0;
  int high = GetSize() - 1;
  int mid = 0;
  while (low <= high) {
    mid = (low + high) / 2;
    if (comparator(array_[mid].first, key) > 0) {  // 括号运算符重载函数的语法：|返回值类型 operator()(参数列表);
      high = mid - 1;
    } else if (comparator(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      search_result->push_back(array_[mid].second);
      return true;
    }
  }
  // 循环走完后说明查找结点不存在
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // replace with your own code
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // replace with your own code
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) const -> const MappingType & {
  // replace with your own code
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &compactor) -> bool {
  // 叶子先插入后分裂。分裂函数放在b+树中实现，结点只负责插入

  if (GetSize() == 1) {
    if (compactor(array_[0].first, key) < 0) {
      SetKeyAt(1, key);
      SetValueAt(1, value);
      IncreaseSize(1);
      return true;
    }
  }
  int low = 0;
  int high = GetSize() - 1;
  int mid;
  while (low <= high) {
    mid = (low + high) / 2;
    if (compactor(array_[mid].first, key) > 0) {
      high = mid - 1;
    } else if (compactor(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      return false;  // 重复的键
    }
  }
  // 循环结束后low指向的位置为第一个比key大的位置，于是从low这个位置前面插入
  CopyBackward(low);
  SetKeyAt(low, key);
  SetValueAt(low, value);
  IncreaseSize(1);
  return true;
}

/*
 * Shift all elements starting from index to right by 1 position
 * so that index end up with an empty hole for insert
 * but will not increase the size, it's up to the caller to do so
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::CopyBackward(int index) {
  std::copy_backward(
      array_ + index, array_ + GetSize(),
      array_ + GetSize() + 1);  // 指针加1表示将指针的值增加1个单位。这个单位取决于指针所指向的数据类型的大小。
}

/*
 * Shift all elements starting from index to left by 1 position
 * essentially cover index-1, assuming it's deleted
 * but will not decrease the size, it's up to the caller to do so
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Copy(int index) {
  std::copy(array_ + index, array_ + GetSize(), array_ + index - 1);
}

/*
 * Delete a pair with the specified key from this leaf page
 * return false if such key doesn't exist, true if successful deletion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(const KeyType &key, KeyComparator &comparator) -> bool {
  // 在叶子结点中的remove只考虑删除结点。删除结点后可能会触发的合并删除放在上层函数中实现
  int low = 0;
  int high = GetSize() - 1;
  int mid;
  bool search_result = false;
  while (low <= high) {
    mid = (low + high) / 2;
    if (comparator(array_[mid].first, key) > 0) {
      high = mid - 1;
    } else if (comparator(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      search_result = true;
      break;
    }
  }
  if (!search_result) {
    return false;
  }
  // mid为要删除的节点位置，将该位置后面的元素往前移，就是mid+1位置及其后的元素向前挪一格到mid位置上
  // 在此之前先进行边界判断,否则copy函数的参数会越界访问
  if (mid < GetSize() - 1) {
    std::copy(array_ + mid + 1, array_ + GetSize(), array_ + mid);
  }

  DecreaseSize(1);
  return true;
}

/*
 * Move All elements in this node to another recipient
 * and adjust size of both sides accordingly
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeTo(BPlusTreeLeafPage *recipient) {
  // 什么时候会用到？合并merge
  auto recipient_size = recipient->GetSize();
  auto size = this->GetSize();
  std::copy(&array_[0], &array_[size], &recipient->array_[recipient_size]);
  this->SetSize(0);
  recipient->IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLatterHalfTo(BPlusTreeLeafPage *recipient) {
  //
  // replace with your own codes 创建新页面放置分裂后的数据
  // round_up_minsize== (n+1)/2
  // 向上取整+向下取整=原数*2
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "Movehalfto GetSize() == GetMaxSize()");
  auto round_up_minsize = GetMaxSize() / 2 + (GetMaxSize() % 2 != 0);
  std::copy(&array_[round_up_minsize], &array_[GetSize()], &recipient->array_[0]);
  SetSize(round_up_minsize);
  recipient->SetSize(GetMaxSize() - round_up_minsize);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // 给左兄弟（向右兄弟）借一个结点 并调整大小
  recipient->SetKeyAt(recipient->GetSize(), this->KeyAt(0));
  recipient->SetValueAt(recipient->GetSize(), this->ValueAt(0));

  std::copy(array_ + 1, array_ + GetSize(), array_);
  // or 调用Copybackward

  DecreaseSize(1);
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // 给右兄弟（向左兄弟）借一个结点

  recipient->CopyBackward(0);
  recipient->SetKeyAt(0, this->KeyAt(GetSize() - 1));
  recipient->SetValueAt(0, this->ValueAt(GetSize() - 1));

  DecreaseSize(1);

  recipient->IncreaseSize(1);
}

/*
 * How many bytes each key-value pair takes
 */

INDEX_TEMPLATE_ARGUMENTS
//
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetMappingSize() const -> size_t {
  // replace with your own code
  //
  return sizeof(MappingType);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArrayPtr() -> char * { return reinterpret_cast<char *>(&array_[0]); }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
