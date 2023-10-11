//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      filter_predicate_(plan->filter_predicate_),
      end_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()),
      iterator_(end_) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  // if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
  //     txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
  // }
  iterator_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(txn);
  // table_name_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // auto txn = exec_ctx_->GetTransaction();
  // auto oid = plan_->GetTableOid();
  if (iterator_ == end_) {
    return false;
  }

  *rid = iterator_->GetRid();
  *tuple = *iterator_++;
  return true;
}
}  // namespace bustub
