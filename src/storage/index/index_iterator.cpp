/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

/*
 * Assume the caller has fetched the leaf page for us, unpin it later on when we are done with it
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, LEAF_TYPE leaf_page,
                                  BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), leaf_page_(leaf_page), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {  // NOLINT
  if (page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->PairAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  index_++;
  if (index_ == leaf_page_->GetSize()) {
    page_id_ = leaf_page_->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      leaf_page_ = nullptr;
      index_ = -1;
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      leaf_page_ = FetchLeafPage(page_id_);
      index_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

/*
 *fetch函数使用前需要判断能否使用
 *无效页面页面不能调用
 */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::FetchLeafPage(page_id_t page_id) -> LEAF_TYPE {
  return reinterpret_cast<LEAF_TYPE>(buffer_pool_manager_->FetchPage(page_id)->GetData());
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
