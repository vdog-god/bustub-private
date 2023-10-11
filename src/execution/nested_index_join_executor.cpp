//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_schema_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_->GetKeySchema()),
      left_schema_(plan_->GetChildPlan()->OutputSchema()) {  // plan_->GetChildPlan()->OutputSchema()
                                                             // 和child_executor->GetOutputSchema()的输出有何不同？
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  // 注：左表也是外表，右表也是内表
  child_executor_->Init();
  index_oid_t oid = plan_->GetIndexOid();
  inner_index_ = exec_ctx_->GetCatalog()->GetIndex(oid);
  inner_table_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 每次从outer_table中获取一个键，循环遍历索引表找出匹配的键输出，若没有匹配的键则输出null或不输出
  RID left_rid;
  while (child_executor_->Next(&outer_tuple_, &left_rid)) {
    // 从left_tuple构建索引键用于在索引中查找
    // 先获取value，在构建成tuple类型的key
    // 最后用这个key在索引中查找
    std::vector<Value> key_val{plan_->KeyPredicate()->Evaluate(&outer_tuple_, left_schema_)};
    // 用索引的schema来构建索引键
    Tuple key_tup = Tuple(key_val, index_schema_);
    std::vector<RID> results{};
    // 由于index中没有重复的键，因此在扫描到一个匹配的值之后，此次扫描结束，不用在往下扫描。而外表（左表）指向下一行继续扫描
    inner_index_->index_->ScanKey(key_tup, &results, exec_ctx_->GetTransaction());
    if (!results.empty()) {
      auto inner_rid = results.back();
      inner_table_ptr_->table_->GetTuple(inner_rid, &inner_tuple_, exec_ctx_->GetTransaction());
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> value;
      MergeValueFromTuple(value, true);  // 右表的null值
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
  }
  return false;
}
}  // namespace bustub
