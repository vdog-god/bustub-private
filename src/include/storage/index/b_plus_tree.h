//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  enum class LatchModes { READ = 0, INSERT, DELETE, OPTIMIZE };
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;
  auto InsertHelper(const KeyType &key, const ValueType &value, Transaction *transaction, LatchModes mode) -> bool;
  void InsertInLeaf(LeafPage *recipient, KeyType key, ValueType value);
  void InsertInParent(BPlusTreePage *recipient, const KeyType &key, BPlusTreePage *recipient_new, int &dirty_height);
  auto CreateInternalPage() -> InternalPage *;
  auto CreateLeafPage() -> LeafPage *;
  void InitBplusTree(KeyType key, ValueType value);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);
  void RemoveHelper(const KeyType &key, Transaction *transaction, LatchModes mode);
  void DeleteEntry(BPlusTreePage *recipient, KeyType key, int &dirty_height);
  auto TryRedistribute(BPlusTreePage *recipient, KeyType key) -> bool;
  void Redistribute(BPlusTreePage *recipient, BPlusTreePage *recipient_brother, InternalPage *parent,
                    int recipient_position, bool brother_on_left);
  auto TryMerge(BPlusTreePage *recipient, KeyType key, int &dirty_height) -> bool;
  void Merge(BPlusTreePage *recipient, BPlusTreePage *recipient_brother, Page *brother_page, InternalPage *parent,
             int recipient_position, bool brother_on_left, int &dirty_height);
  void UpdateAllParentID(InternalPage *recipient);
  void UpdateParentID(InternalPage *recipient, int index);
  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  auto FetchBPlusTreePage(page_id_t page_id) -> std::pair<Page *, BPlusTreePage *>;

  auto ReinterpretAsLeafPage(BPlusTreePage *page) -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> *;

  auto ReinterpretAsInternalPage(BPlusTreePage *page) -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *;

  auto FindLeafPage(const KeyType &key, Transaction *transaction = nullptr, LatchModes mode = LatchModes::READ)
      -> std::pair<Page *, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>;

  void LatchRootPageId(Transaction *transaction, BPlusTree::LatchModes mode);

  void ReleaseAllLatches(Transaction *transaction, LatchModes mode, int dirty_height = 0);

  auto IsSafePage(BPlusTreePage *page, LatchModes mode) -> bool;
  void SetPageDirty(page_id_t page_id);
  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  // 用来记录树的根节点被创建
  bool header_record_created_{false};
  // latch for the root_id
  ReaderWriterLatch root_id_rwlatch_;
};

}  // namespace bustub
