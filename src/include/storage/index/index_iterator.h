//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
#define LEAF_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t page_id, int index, LEAF_TYPE leaf_page, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // add your own private member variables here
  auto FetchLeafPage(page_id_t page_id) -> LEAF_TYPE;

  page_id_t page_id_{INVALID_PAGE_ID};
  int index_{-1};
  LEAF_TYPE leaf_page_{nullptr};
  BufferPoolManager *buffer_pool_manager_{nullptr};
};

}  // namespace bustub
