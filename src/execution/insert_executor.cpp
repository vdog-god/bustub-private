//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  insert_result_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // insert 有一个子executor节点，这个child——executor有自己的next函数，为当前算子提供tuple
  if (insert_result_) {
    return false;
  }
  int count = 0;
  Tuple child_tuple{};
  RID child_rid{};

  auto table_indexes =
      exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_);
  // 用于在每个索引上遍历插入key
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);  // 插入tuple
  auto tuple_schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&child_tuple, rid)) {
    auto insert_result = table_info->table_->InsertTuple(child_tuple, &child_rid, exec_ctx_->GetTransaction());
    if (insert_result) {
      ++count;
    }
    if (!table_indexes.empty() && insert_result) {
      std::for_each(table_indexes.begin(), table_indexes.end(), [&](auto lt) {
        auto key_schema = lt->index_->GetKeySchema();
        auto key_attrs = lt->index_->GetKeyAttrs();
        auto key = child_tuple.KeyFromTuple(tuple_schema, *key_schema, key_attrs);
        lt->index_->InsertEntry(key, child_rid, exec_ctx_->GetTransaction());
      });
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, count);
  Tuple tuple_tmp = Tuple(values, &GetOutputSchema());
  *tuple = tuple_tmp;
  insert_result_ = true;
  return true;
}

}  // namespace bustub
