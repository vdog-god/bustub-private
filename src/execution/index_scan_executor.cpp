//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // 获取相应的b+树索引

  auto index_oid = plan_->GetIndexOid();

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid);

  index_tree_ =
      dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());  // 获取独占指针的裸指针,注意释放

  indicator_ = index_tree_->GetBeginIterator();
  end_ = index_tree_->GetEndIterator();
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();  // 独占指针
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (indicator_ == end_) {
    return false;
  }

  *rid = (*indicator_).second;  // 有了rid可以通过table_heap_索引到相应的tuple
  table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++indicator_;  // 为何indicator_++报错。
  return true;
}

}  // namespace bustub
