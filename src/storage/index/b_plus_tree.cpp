#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS  // 构造函数
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // LOG_DEBUG("leaf-size %d , inter-size %d", leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; } /*存疑*/
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // for consistency, make sure there is always a non-empty transaction passed in
  //
  bool create_transcation = false;
  if (transaction == nullptr) {
    transaction = new Transaction(0);
    create_transcation = true;
  }

  LatchRootPageId(transaction, LatchModes::READ);

  if (IsEmpty()) {
    ReleaseAllLatches(transaction, LatchModes::READ);
    if (create_transcation) {
      delete transaction;
    }
    return false;
  }

  BPlusTreeLeafPage<KeyType, RID, KeyComparator> *leaf_page = FindLeafPage(key, transaction, LatchModes::READ).second;

  auto found = leaf_page->BinarySearch(key, result, comparator_);
  ReleaseAllLatches(transaction, LatchModes::READ);
  if (create_transcation) {  // 若事物是函数内申请的，则在函数结束时释放
    delete transaction;
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  bool create_transaction = false;
  if (transaction == nullptr) {
    transaction = new Transaction(0);
    create_transaction = true;
  }
  bool success = InsertHelper(key, value, transaction, LatchModes::OPTIMIZE);
  // LOG_DEBUG("insert  %lld", key.ToString());
  if (create_transaction) {
    delete transaction;
  }
  return success;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertHelper(const KeyType &key, const ValueType &value, Transaction *transaction, LatchModes mode)
    -> bool {
  LatchRootPageId(transaction, mode);
  int dirty_height = 0;
  if (this->IsEmpty()) {
    // 如果树空，需要上写锁来初始化,需要改变模式
    if (mode == LatchModes::OPTIMIZE) {
      ReleaseAllLatches(transaction, mode);
      return InsertHelper(key, value, transaction, LatchModes::INSERT);
    }
    InitBplusTree(key, value);
    ReleaseAllLatches(transaction, mode, dirty_height);
    return true;
  }

  // 先乐观锁插入，若不安全则悲观锁插入
  auto [leaf_page_raw, leaf_page] = FindLeafPage(key, transaction, mode);
  std::vector<ValueType> search_result;
  bool repetition = leaf_page->BinarySearch(key, &search_result, comparator_);
  if (repetition) {
    ReleaseAllLatches(transaction, mode, dirty_height);
    return false;
  }
  if ((leaf_page->GetSize() == (leaf_max_size_ - 1)) && (mode == LatchModes::OPTIMIZE)) {
    // 乐观锁失效
    ReleaseAllLatches(transaction, mode);
    return InsertHelper(key, value, transaction, LatchModes::INSERT);
  }
  // 叶节点插入后触发页满的判断条件，当插入之后叶面大小为n时会分裂。换句话说，也就是插入之后比n小不会分裂or插入之前为n-1则插入后会分裂
  dirty_height += 1;
  if (leaf_page->GetSize() < (leaf_max_size_ - 1)) {
    InsertInLeaf(leaf_page, key, value);
    // 并发后在锁管理器中unpin该叶子页面  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  } else {
    // 由于没有新创建内存区，所以可能插入的时候叶子会越界？
    // 同时和删除并发的时候可能会出错（和copy函数有关）？但是删除函数已经加锁了。但插入的时候叶子也被加锁不应该会越界啊
    auto leaf_page_new = CreateLeafPage();
    leaf_page_new->SetParentPageId(leaf_page->GetParentPageId());
    leaf_page->Insert(key, value, comparator_);
    leaf_page->MoveLatterHalfTo(leaf_page_new);
    // 如果分裂条件设置为插入后为n则进行分裂，那是不是变相说明插入前为n-1为满。那么最大值是不是变相为maxsize-1。
    const auto key_upward = leaf_page_new->KeyAt(0);
    InsertInParent(leaf_page, key_upward, leaf_page_new, dirty_height);
    buffer_pool_manager_->UnpinPage(leaf_page_new->GetPageId(), true);
    // 在并发过程中，原先要分裂的叶子页面交给锁管理器释放。buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),
    // true);
  }
  ReleaseAllLatches(transaction, mode, dirty_height);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeaf(LeafPage *recipient, KeyType key, ValueType value) {
  recipient->Insert(key, value, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *recipient, const KeyType &key, BPlusTreePage *recipient_new,
                                    int &dirty_height) {
  dirty_height++;
  if (recipient->IsRootPage()) {
    auto root_node = CreateInternalPage();
    root_page_id_ = root_node->GetPageId();  // 每次修改完根节点以后都要更新索引
    UpdateRootPageId();
    root_node->SetValueAt(0, recipient->GetPageId());
    root_node->SetKeyAt(1, key);
    root_node->SetValueAt(1, recipient_new->GetPageId());
    root_node->IncreaseSize(1);
    recipient->SetParentPageId(root_node->GetPageId());
    recipient_new->SetParentPageId(root_node->GetPageId());
    buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
    return;
  }

  auto parent_node_id = recipient->GetParentPageId();
  auto [parent_buffer_page, raw_parent_node] = FetchBPlusTreePage(parent_node_id);
  auto parent_node = ReinterpretAsInternalPage(raw_parent_node);

  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(key, recipient_new->GetPageId(), comparator_);
  } else {
    // 创建能容纳n+1 的内存T区
    auto *mem_t = new char[INTERNAL_PAGE_HEADER_SIZE + (internal_max_size_ + 1) * sizeof(MappingType)];
    auto *new_internal_t = reinterpret_cast<InternalPage *>(mem_t);
    // memset(mem_t, 0, INTERNAL_PAGE_HEADER_SIZE+(internal_max_size_+1)*sizeof(MappingType))
    std::memcpy(mem_t, parent_buffer_page->GetData(),
                INTERNAL_PAGE_HEADER_SIZE + (internal_max_size_) * sizeof(MappingType));
    new_internal_t->Insert(key, recipient_new->GetPageId(), comparator_);

    auto parent_node_new = CreateInternalPage();
    parent_node_new->SetParentPageId(parent_node->GetParentPageId());
    // Copy T.P1 … T.P⌈(n+1)∕2⌉ into P
    std::copy(new_internal_t->GetArray(), new_internal_t->GetArray() + (internal_max_size_ + 2) / 2,
              parent_node->GetArray());
    parent_node->SetSize(internal_max_size_ / 2 + 1);
    // Let K′′=  T.K⌈(n+1)∕2⌉
    auto key_upward = new_internal_t->KeyAt(internal_max_size_ / 2 + 1);
    // Copy T.P⌈(n+1)∕2⌉+1 … T.Pn+1 into P′
    std::copy(new_internal_t->GetArray() + internal_max_size_ / 2 + 1,
              new_internal_t->GetArray() + internal_max_size_ + 1, parent_node_new->GetArray());
    parent_node_new->SetSize((internal_max_size_ + 1) / 2);

    UpdateAllParentID(parent_node_new);
    delete[] mem_t;
    InsertInParent(parent_node, key_upward, parent_node_new, dirty_height);
    buffer_pool_manager_->UnpinPage(parent_node_new->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_buffer_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateInternalPage() -> InternalPage * {
  auto p_id = INVALID_PAGE_ID;
  auto parent_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_page->Init(p_id, parent_id, internal_max_size_);
  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafPage() -> LeafPage * {
  page_id_t p_id = INVALID_PAGE_ID;
  page_id_t parent_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_page->Init(p_id, parent_id, leaf_max_size_);
  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitBplusTree(const KeyType key, const ValueType value) {
  /*
   * Ask for a new page from bpm (which will have pin count = 1)
   * and set this page to be B+ Leaf Page and insert first key-value pair into it
   * update the header page root_idx and unpin this page with dirty flag
   */
  auto bplus_root = CreateLeafPage();
  root_page_id_ = bplus_root->GetPageId();
  UpdateRootPageId(!header_record_created_);
  header_record_created_ = true;
  bplus_root->SetKeyAt(0, key);
  bplus_root->SetValueAt(0, value);
  bplus_root->IncreaseSize(1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  bool create_transaction = false;
  if (transaction == nullptr) {
    transaction = new Transaction(0);
    create_transaction = true;
  }
  // LOG_DEBUG("remove  %lld", key.ToString());
  RemoveHelper(key, transaction, LatchModes::OPTIMIZE);
  if (create_transaction) {
    delete transaction;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveHelper(const KeyType &key, Transaction *transaction, LatchModes mode) {
  LatchRootPageId(transaction, mode);
  if (IsEmpty()) {
    ReleaseAllLatches(transaction, mode);
    return;
  }
  int dirty_height = 0;
  auto [raw_curr_node, curr_node] = FindLeafPage(key, transaction, mode);
  if (curr_node->GetSize() == curr_node->GetMinSize() && mode == LatchModes::OPTIMIZE) {
    auto is_root = curr_node->IsRootPage();
    auto fail_condition1 = !is_root;
    auto fail_condition2 = is_root && ((curr_node->GetSize() - 1) == 0);
    if (fail_condition1 || fail_condition2) {
      ReleaseAllLatches(transaction, mode);
      return RemoveHelper(key, transaction, LatchModes::DELETE);
    }
  }
  DeleteEntry(curr_node, key, dirty_height);
  ReleaseAllLatches(transaction, mode, dirty_height);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *recipient, KeyType key, int &dirty_height) {
  // 依据传入的结点类型对key进行删除
  if (recipient->IsLeafPage()) {
    // LOG_DEBUG("leaf,key is %lld", key.ToString());
    // 如果节点删除失败则不应该继续下面的递归循环
    //
    auto delete_result = ReinterpretAsLeafPage(recipient)->RemoveKey(key, comparator_);
    // LOG_DEBUG("delete ... %d", delete_result);
    if (!delete_result) {
      return;
    }
    // ReinterpretAsLeafPage(recipient)->RemoveKey(key, comparator_);
  } else {
    // LOG_DEBUG("internal,key is %lld", key.ToString());
    // ReinterpretAsInternalPage(recipient)->RemoveKey(key, comparator_);
    auto delete_result = ReinterpretAsInternalPage(recipient)->RemoveKey(key, comparator_);
    // LOG_DEBUG("delete ... %d", delete_result);
    if (!delete_result) {
      return;
    }
  }
  dirty_height++;
  // 重分配或合并前应该满足不得不进行分配和合并的条件
  if (recipient->GetSize() >= recipient->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(recipient->GetPageId(), true);
    return;
  }

  // 需要对结构进行调整.当进行到合并的过程时，由于会删除父结点中的索引指针，因此会向上递归看是其父结点是否会进行合并，重分配，或是根节点。
  if (recipient->IsRootPage()) {
    if (recipient->IsInternalPage()) {
      if (recipient->GetSize() == 1) {
        root_page_id_ = ReinterpretAsInternalPage(recipient)->ValueAt(0);
        UpdateRootPageId(false);
        BPlusTreePage *new_root_page = FetchBPlusTreePage(root_page_id_).second;
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
      }
    } else {
      if (recipient->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();  // 默认false
      }
    }
  } else {
    // follow the order of
    // 1. redistribute from right
    // 2. redistribute from left
    // 3. merge from right
    // 4. merge from left
    auto redistibute_result = TryRedistribute(recipient, key);
    if (!redistibute_result) {
      TryMerge(recipient, key, dirty_height);
    }
    // bool too_small = curr_page->GetSize() <= curr_page->GetMinSize();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryRedistribute(BPlusTreePage *recipient, KeyType key) -> bool {
  // 确定父结点和兄弟结点
  auto parent_node_id = recipient->GetParentPageId();
  BPlusTreePage *parent_node_header = FetchBPlusTreePage(parent_node_id).second;
  auto parent_node = ReinterpretAsInternalPage(parent_node_header);
  int recipient_position = parent_node->BinarySearch(key, comparator_).first;
  auto redistribute_result = false;
  // 此处为逻辑框架//判断是左兄弟还是右兄弟。
  // if(reicipient_position<parent_node->GetSize()-1){
  //   //此时一定有右兄弟，先尝试和右兄弟合并，不成功则和左兄弟合并

  // }else if(reicipient_position==parent_node->GetSize()-1) {
  // //此时没有右兄弟，只能和左兄弟合并
  // }
  if (recipient_position < parent_node->GetSize() - 1) {
    // 一定有右兄弟.先和右兄弟重分配,不成功后和左兄弟重分配
    auto position = parent_node->ValueAt(recipient_position + 1);
    auto [brother_node_raw, brother_node_header] = FetchBPlusTreePage(position);
    brother_node_raw->WLatch();
    if (brother_node_header->GetSize() > brother_node_header->GetMinSize()) {
      // 保证兄弟结点能借
      Redistribute(recipient, brother_node_header, parent_node, recipient_position, false);
      redistribute_result = true;
    }
    brother_node_raw->WUnlatch();
    buffer_pool_manager_->UnpinPage(brother_node_header->GetPageId(), redistribute_result);
  }
  // 若以上重分配没进行说明要么position=current size 即只有左兄弟 或者说明右兄弟无法进行重分配。

  if ((recipient_position > 0) && (recipient_position <= (parent_node->GetSize() - 1)) && !redistribute_result) {
    // 位置合法，且没进行过右兄弟分配或右兄弟分配失败
    auto position = parent_node->ValueAt(recipient_position - 1);
    auto [brother_node_raw, brother_node_header] = FetchBPlusTreePage(position);
    brother_node_raw->WLatch();
    if (brother_node_header->GetSize() > brother_node_header->GetMinSize()) {
      // 保证兄弟结点能借
      Redistribute(recipient, brother_node_header, parent_node, recipient_position, true);
      redistribute_result = true;
    }
    brother_node_raw->WUnlatch();
    buffer_pool_manager_->UnpinPage(brother_node_header->GetPageId(), redistribute_result);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), redistribute_result);
  return redistribute_result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *recipient, BPlusTreePage *recipient_brother, InternalPage *parent,
                                  int recipient_position, bool brother_on_left) {
  if (recipient->IsLeafPage()) {
    // 对于叶节点 简单左右移动key以后将结点中的第一个或最后一个位置的key提升到父结点中
    auto curr_node = ReinterpretAsLeafPage(recipient);
    auto brother_node = ReinterpretAsLeafPage(recipient_brother);
    if (brother_on_left) {
      brother_node->MoveLastToFrontOf(curr_node);
      parent->SetKeyAt(recipient_position, curr_node->KeyAt(0));
    } else {
      brother_node->MoveFirstToEndOf(curr_node);
      parent->SetKeyAt(recipient_position + 1, brother_node->KeyAt(0));
    }
  } else {
    // 对于内部结点
    // 结点重分配时，需要在父结点和孩子结点之间转移key。同时重分配完成后还需要重设孩子结点中指向父结点的指针，时其指向正确的新分配后的父节点
    auto curr_node = ReinterpretAsInternalPage(recipient);
    auto brother_node = ReinterpretAsInternalPage(recipient_brother);
    if (brother_on_left) {
      auto key_up = brother_node->KeyAt(brother_node->GetSize() - 1);
      brother_node->MoveLastToFrontOf(curr_node);
      UpdateParentID(curr_node, 0);
      curr_node->SetKeyAt(1, parent->KeyAt(recipient_position));
      parent->SetKeyAt(recipient_position, key_up);
    } else {
      auto key_up = brother_node->KeyAt(1);
      brother_node->MoveFirstToEndOf(curr_node);
      UpdateParentID(curr_node, curr_node->GetSize() - 1);
      curr_node->SetKeyAt(curr_node->GetSize() - 1, parent->KeyAt(recipient_position + 1));
      parent->SetKeyAt(recipient_position + 1, key_up);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryMerge(BPlusTreePage *recipient, KeyType key, int &dirty_height) -> bool {
  // 确定父结点和兄弟结点
  auto parent_node_id = recipient->GetParentPageId();
  BPlusTreePage *raw_parent_node = FetchBPlusTreePage(parent_node_id).second;
  auto parent_node = ReinterpretAsInternalPage(raw_parent_node);
  int recipient_position = parent_node->BinarySearch(key, comparator_).first;
  auto merge_result = false;

  if (recipient_position < parent_node->GetSize() - 1) {
    // 一定有右兄弟.先和右兄弟合并,不成功后和左兄弟合并
    auto b_position = parent_node->ValueAt(recipient_position + 1);
    auto [brother_node_raw, brother_node_header] = FetchBPlusTreePage(b_position);
    brother_node_raw->WLatch();  // 在merge中释放锁
    Merge(recipient, brother_node_header, brother_node_raw, parent_node, recipient_position, false, dirty_height);
    merge_result = true;
    buffer_pool_manager_->UnpinPage(brother_node_header->GetPageId(), merge_result);
  }
  // 经历过重分配失败以后，合并必然会成功，除非当前位置没有右兄弟，才会合并失败。

  if ((recipient_position > 0) && (recipient_position <= (parent_node->GetSize() - 1)) && !merge_result) {
    // 位置合法，且没进行过右兄弟分配或右兄弟分配失败
    auto b_position = parent_node->ValueAt(recipient_position - 1);
    auto [brother_node_raw, brother_node_header] = FetchBPlusTreePage(b_position);
    brother_node_raw->WLatch();
    Merge(recipient, brother_node_header, brother_node_raw, parent_node, recipient_position, true, dirty_height);
    merge_result = true;
    buffer_pool_manager_->UnpinPage(brother_node_header->GetPageId(), merge_result);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),
                                  merge_result);  // ”谁借谁还“ 谁申请的内存页面，使用完毕后由谁归还
  return merge_result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *recipient, BPlusTreePage *recipient_brother, Page *brother_page,
                           InternalPage *parent, int recipient_position, bool brother_on_left, int &dirty_height) {
  // 合并的过程主要细节为：1.判断根叶节点2.判断左合并还是有合并3.递归调用4.合并后废弃节点的处理
  if (recipient->IsLeafPage()) {
    // 对于叶节点 需要额外考虑nextpageid的重设。
    auto curr_node = ReinterpretAsLeafPage(recipient);
    auto brother_node = ReinterpretAsLeafPage(recipient_brother);
    if (brother_on_left) {
      // 合并时默认结点中大的元素排在小的元素后面
      curr_node->MergeTo(brother_node);
      brother_node->SetNextPageId(curr_node->GetNextPageId());
      brother_page->WUnlatch();
      //  curr_node->SetParentPageId(INVALID_PAGE_ID);
      // 这个空页面的锁也应该交由锁管理器释放 同时被unpim
      // buffer_pool_manager_->UnpinPage(curr_node->GetPageId(), false);
      DeleteEntry(parent, parent->KeyAt(recipient_position), dirty_height);
    } else {
      brother_node->MergeTo(curr_node);
      curr_node->SetNextPageId(brother_node->GetNextPageId());
      // brother_node->SetParentPageId(INVALID_PAGE_ID);
      brother_page->WUnlatch();
      // buffer_pool_manager_->UnpinPage(brother_node->GetPageId(), false);
      DeleteEntry(parent, parent->KeyAt(recipient_position + 1), dirty_height);
    }
  } else {
    // 对于内部结点
    // 结点重分配时，需要在父结点和孩子结点之间转移key.同时需要更新结点合并后该结点新合并进来的元素的父结点id。用updateparentID完成
    // 合并后需要重设指向父结点的指针，保证指向合并后的正确父结点。同时无需设置nextpageid

    auto curr_node = ReinterpretAsInternalPage(recipient);
    auto brother_node = ReinterpretAsInternalPage(recipient_brother);
    if (brother_on_left) {
      auto old_brother_size = brother_node->GetSize();
      curr_node->MergeTo(brother_node);
      UpdateAllParentID(brother_node);
      brother_node->SetKeyAt(old_brother_size, parent->KeyAt(recipient_position));
      // curr_node->SetParentPageId(INVALID_PAGE_ID);
      brother_page->WUnlatch();
      // 非兄弟页面一律交给锁管理器解锁，兄弟页面谁申请的谁解锁
      // buffer_pool_manager_->UnpinPage(curr_node->GetPageId(), false);
      // LOG_DEBUG("leftbrother %lld", parent->KeyAt(recipient_position).ToString());
      DeleteEntry(parent, parent->KeyAt(recipient_position), dirty_height);
    } else {
      auto old_curr_size = curr_node->GetSize();
      brother_node->MergeTo(curr_node);
      UpdateAllParentID(curr_node);
      // brother_node->SetParentPageId(INVALID_PAGE_ID);
      curr_node->SetKeyAt(old_curr_size, parent->KeyAt(recipient_position + 1));
      brother_page->WUnlatch();
      // buffer_pool_manager_->UnpinPage(brother_node->GetPageId(), false);
      // LOG_DEBUG("rightbrother %lld", parent->KeyAt(recipient_position).ToString());
      DeleteEntry(parent, parent->KeyAt(recipient_position + 1), dirty_height);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentID(InternalPage *recipient, int index) {
  auto parent_id = recipient->GetPageId();
  BPlusTreePage *refresh_nodes = FetchBPlusTreePage(recipient->ValueAt(index)).second;
  refresh_nodes->SetParentPageId(parent_id);
  buffer_pool_manager_->UnpinPage(recipient->ValueAt(index), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateAllParentID(InternalPage *recipient) {
  auto parent_id = recipient->GetPageId();
  for (int i = 0; i < recipient->GetSize(); i++) {
    auto refresh_nodes = FetchBPlusTreePage(recipient->ValueAt(i)).second;
    if (refresh_nodes->GetParentPageId() != parent_id) {
      refresh_nodes->SetParentPageId(parent_id);
    }
    buffer_pool_manager_->UnpinPage(refresh_nodes->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  auto [raw_leaf_page, leaf_page] = FindLeafPage(KeyType());
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), 0, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto [raw_leaf_page, leaf_page] = FindLeafPage(key);
  auto index_position = leaf_page->SearchPosition(key, comparator_);
  if (index_position == -1) {
    // 若index_position为-1，说明当前页面所有元素的key都比给定的key小，迭代器只能指向下个页面的首元素
    auto leaf_id = leaf_page->GetPageId();
    auto next_page_id = leaf_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_id, false);
    if (next_page_id == INVALID_PAGE_ID) {
      return End();
    }
    leaf_page = ReinterpretAsLeafPage(FetchBPlusTreePage(next_page_id).second);
    index_position = 0;
  }

  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), index_position, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }
/*
 * In concurrent mode, need to grab latch on the root id
 * before fetching the root page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LatchRootPageId(Transaction *transaction, BPlusTree::LatchModes mode) {
  if (mode == LatchModes::READ || mode == LatchModes::OPTIMIZE) {
    root_id_rwlatch_.RLock();
  } else {
    root_id_rwlatch_.WLock();
  }
  // nullptr as indicator for page id latch
  transaction->AddIntoPageSet(nullptr);
}

/** Release all the latches held in the transaction so far
 *  nullptr is treated as root_id_rwlatch_
 *  unlock according to the mode specified
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLatches(Transaction *transaction, LatchModes mode, int dirty_height) {
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    // release from upstream to downstream
    auto front_page = page_set->front();
    auto is_leaf =
        (front_page == nullptr) ? false : reinterpret_cast<BPlusTreePage *>(front_page->GetData())->IsLeafPage();
    if (mode == LatchModes::READ) {
      if (front_page == nullptr) {
        root_id_rwlatch_.RUnlock();
      } else {
        front_page->RUnlatch();
        // LOG_DEBUG("ru %d", front_page->GetPageId());
      }
    } else if (mode == LatchModes::OPTIMIZE) {
      if (front_page == nullptr) {
        root_id_rwlatch_.RUnlock();
      } else {
        // in optimize mode, the last leaf page is held a Wlatch......????why
        if (is_leaf) {
          front_page->WUnlatch();
          // LOG_DEBUG("wu- %d", front_page->GetPageId());
        } else {
          front_page->RUnlatch();
          // LOG_DEBUG("ru-o %d", front_page->GetPageId());
        }
      }
    } else {
      // write mode
      if (front_page == nullptr) {
        root_id_rwlatch_.WUnlock();
      } else {
        front_page->WUnlatch();
        // LOG_DEBUG("wu %d", front_page->GetPageId());
      }
    }
    if (front_page != nullptr) {
      buffer_pool_manager_->UnpinPage(front_page->GetPageId(), page_set->size() <= static_cast<size_t>(dirty_height));
    }
    page_set->pop_front();
  }
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}
/*****************************************************************************
 * Custommize Part
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchBPlusTreePage(page_id_t page_id) -> std::pair<Page *, BPlusTreePage *> {
  auto page_ptr = buffer_pool_manager_->FetchPage(page_id);
  auto tree_page_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  return {page_ptr, tree_page_ptr};
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReinterpretAsLeafPage(BPlusTreePage *page) -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReinterpretAsInternalPage(BPlusTreePage *page)
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  return reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
}

/**
 * Iterate through the B+ Tree to fetch a leaf page
 * the caller should unpin the leaf page after usage
 * @return pointer to a leaf page if found, nullptr otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction, BPlusTree::LatchModes mode)
    -> std::pair<Page *, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *> {
  // BUSTUB_ASSERT(transaction != nullptr, "transction==nullptr");
  auto [curr_page_raw, curr_page] = FetchBPlusTreePage(root_page_id_);

  decltype(curr_page) next_page = nullptr;
  decltype(curr_page_raw) next_page_raw = nullptr;

  if (transaction != nullptr) {
    if (mode == LatchModes::READ) {
      curr_page_raw->RLatch();
      // LOG_DEBUG("r %d", curr_page->GetPageId());
    } else if (mode == LatchModes::OPTIMIZE) {  // 乐观锁中只对叶子上写锁
      if (curr_page->IsLeafPage()) {
        curr_page_raw->WLatch();
        // LOG_DEBUG("w- %d", curr_page->GetPageId());
      } else {
        // LOG_DEBUG("try lock %d", curr_page->GetPageId());
        curr_page_raw->RLatch();
        // LOG_DEBUG("r-o %d", curr_page->GetPageId());
      }
    } else {
      // LOG_DEBUG("try %d", curr_page->GetPageId());
      curr_page_raw->WLatch();
      // LOG_DEBUG("w %d", curr_page->GetPageId());
    }
    if (IsSafePage(curr_page, mode)) {
      ReleaseAllLatches(transaction, mode);  // 在addintopageset函数前释放锁，不会把当前的锁释放掉
    }
    transaction->AddIntoPageSet(curr_page_raw);
  }
  // 递归向下遍历
  while (!curr_page->IsLeafPage()) {
    page_id_t next_page_id = ReinterpretAsInternalPage(curr_page)->BinarySearch(key, comparator_).second;
    auto next_page_pair = FetchBPlusTreePage(next_page_id);
    // 并发后unpinPage放在并发判断的逻辑当中执行
    //  buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    next_page = next_page_pair.second;
    next_page_raw = next_page_pair.first;
    // 添加锁和事务
    if (transaction != nullptr) {
      if (mode == LatchModes::READ) {
        next_page_raw->RLatch();
        // LOG_DEBUG("r %d", next_page->GetPageId());
      } else if (mode == LatchModes::OPTIMIZE) {
        if (next_page->IsLeafPage()) {
          next_page_raw->WLatch();
          // LOG_DEBUG("w-o %d", next_page->GetPageId());
        } else {
          next_page_raw->RLatch();
          // LOG_DEBUG("r-o %d", next_page->GetPageId());
        }
      } else {
        next_page_raw->WLatch();
        // LOG_DEBUG("w %d", next_page->GetPageId());
      }
      if (IsSafePage(next_page, mode)) {
        ReleaseAllLatches(transaction, mode);
      }
      transaction->AddIntoPageSet(next_page_raw);
    } else {
      buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);  // 放置事务为空时没有unpin页面
    }

    curr_page = next_page;
    curr_page_raw = next_page_raw;
  }
  return {curr_page_raw, ReinterpretAsLeafPage(curr_page)};
}
/*
 * Depending on the mode (READ/INSERT/DELETE)
 * decide if this page is safe so that all previous latches could be released at this point
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafePage(BPlusTreePage *page, LatchModes mode) -> bool {
  if (mode == LatchModes::READ || mode == LatchModes::OPTIMIZE) {
    return true;
  }
  if (mode == LatchModes::INSERT) {
    if (page->IsLeafPage()) {
      return static_cast<bool>(page->GetSize() < (page->GetMaxSize() - 1));
    }
    return static_cast<bool>(page->GetSize() < page->GetMaxSize());
  }
  if (mode == LatchModes::DELETE) {
    if (page->GetSize() > page->GetMinSize()) {
      return true;
    }
    if (page->GetSize() <= page->GetMinSize()) {
      // maybe
      // safe，如果兄弟够借则安全，兄弟不够则不安全。同时根节点特殊，根节点且内部节点则size>2安全；根且叶子size>1则一定安全
      if (page->IsRootPage()) {
        // if (page->IsInternalPage() && (page->GetSize() > 2)) {
        //   return true;
        // }
        if (page->IsLeafPage() && (page->GetSize() > 1)) {
          return true;
        }
      }
      return false;
    }
  }
  BUSTUB_ASSERT(false, "Not supposed to hit this default return branch in IsSafePage()");
  return true;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    // LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    // LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
