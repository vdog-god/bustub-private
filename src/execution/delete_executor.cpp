//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "execution/executors/delete_executor.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_result_) {
    return false;
  }
  int count = 0;
  auto table_oid = plan_->table_oid_;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {
    if (table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      // 更新索引信息.表名和表info一一对应
      auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto index_info : indexes_info) {
        auto key = (&child_tuple)
                       ->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                      index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      }
      ++count;
    }
  }
  std::vector<Value> values;
  values.emplace_back(INTEGER, count);  // emplace_back()就地构造。
  auto tuple_tmp = Tuple(values, &this->GetOutputSchema());
  *tuple = tuple_tmp;
  delete_result_ = true;
  return true;
}

}  // namespace bustub
