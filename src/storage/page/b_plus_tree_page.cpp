//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include "common/config.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
auto BPlusTreePage::IsInternalPage() const -> bool { return page_type_ == IndexPageType::INTERNAL_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }
void BPlusTreePage::DecreaseSize(int amount) { size_ -= amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsRootPage()) {
    return IsInternalPage()
               ? 2
               : 1; /*是内部节点则最小键值对数应该为2，而叶子结点应该为1，这是因为内部节点的第一个结点被视为无效*/
  }
  if (IsLeafPage()) {
    return max_size_ /
           2;  // 叶子结点向下取整的原因在于
               // 留出一个位置有利于分裂。因为叶子分裂条件是插入后为max_size时进行分裂，对应于插入前为maxsize-1时分裂，也就是相当于maxsize-1为叶节点的实际最大值
  }
  return (max_size_ + 1) /
         2;  // 内部页面需满足m叉b+树的最大子树为m，最小子树为⎡m/2⎤，而叶子结点无需满足b+树定义。因此为m/2
}

/*
 * Helper methods to get/set parent page id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}  // namespace bustub
