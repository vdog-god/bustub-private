//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/index/generic_key.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(1);  // the first entry with invalid key and negative infinity pointer
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * Given a key,find the jump index for which the traversal could proceed and the next level page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearch(KeyType key, KeyComparator &comparactor) const
    -> std::pair<int, ValueType> {
  // internalPage 得到下个页面key的索引值和下个页面的id
  int low = 1;
  int high = GetSize() - 1;
  int mid;
  while (low <= high) {
    mid = (low + high) / 2;
    if (comparactor(array_[mid].first, key) > 0) {  // 括号运算符重载函数的语法：|返回值类型 operator()(参数列表);
      high = mid - 1;
    } else if (comparactor(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      return {mid, array_[mid].second};
    }
  }
  // if (low == mid && low - 1 == high) {
  //   return array_[low - 1].second;
  // }
  // if (high == mid && low - 1 == high) {
  //   return array_[low - 1].second;
  // 无论哪种情况
  // low-1这个位置上的value是正确的下个节点id。当查找的key比节点中所有的key都大的时候会触发low=mid+1，此时low-1=high=mid，访问
  return {low - 1, array_[low - 1].second};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  // 叶子先插入后分裂。分裂函数放在b+树中实现，结点只负责插入
  int low = 1;
  int high = GetSize() - 1;
  int mid;
  while (low <= high) {
    mid = (low + high) / 2;
    if (comparator(array_[mid].first, key) > 0) {
      high = mid - 1;
    } else if (comparator(array_[mid].first, key) < 0) {
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKey(const KeyType &key, KeyComparator &comparator) -> bool {
  // 在叶子结点中的remove只考虑删除结点。删除结点后可能会触发的合并删除放在上层函数中实现
  int low = 1;
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
  if (mid < GetSize() - 1) {
    std::copy(array_ + mid + 1, array_ + GetSize(), array_ + mid);
  }
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeTo(BPlusTreeInternalPage *recipient) {
  // 什么时候会用到？删除合并
  auto recipient_size = recipient->GetSize();
  auto size = this->GetSize();
  std::copy(&array_[0], &array_[size], &recipient->array_[recipient_size]);
  SetSize(0);
  recipient->IncreaseSize(size);
}
/*
 * Shift all elements starting from index to right by 1 position
 * so that index end up with an empty hole for insert
 * but will not increase the size, it's up to the caller to do so
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyBackward(int index) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(int index) {
  std::copy(array_ + index, array_ + GetSize(), array_ + index - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  // 给左兄弟（向右兄弟）借一个结点 并调整大小
  recipient->SetKeyAt(recipient->GetSize(), this->KeyAt(0));
  recipient->SetValueAt(recipient->GetSize(), this->ValueAt(0));

  std::copy(array_ + 1, array_ + GetSize(), array_);
  // or 调用Copybackward
  DecreaseSize(1);
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  // 给右兄弟（向左兄弟）借一个结点
  recipient->CopyBackward(0);
  recipient->SetKeyAt(0, this->KeyAt(GetSize() - 1));
  recipient->SetValueAt(0, this->ValueAt(GetSize() - 1));
  DecreaseSize(1);
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() -> MappingType * { return (&array_[0]); }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
