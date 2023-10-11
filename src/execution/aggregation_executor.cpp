//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()),
      end_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();
  // Aggregation 需要在 Init() 中直接计算出全部结果，将结果暂存，
  // 再在 Next() 中一条一条地 emit。而 SimpleAggregationHashTable 就是计算并保存 Aggregation 结果的数据结构。
  empty_output_ = true;
  Tuple tuple_tmp{};
  RID rid{};
  auto next_child = child_->Next(&tuple_tmp, &rid);
  while (next_child) {
    auto key = MakeAggregateKey(&tuple_tmp);
    auto aggregate_value = MakeAggregateValue(&tuple_tmp);
    aht_.InsertCombine(key, aggregate_value);
    next_child = child_->Next(&tuple_tmp, &rid);
  }
  // 表初始化后再初始化迭代器
  aht_iterator_ = aht_.Begin();
  end_ = aht_.End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // next吐出一个聚合结果，吐出的格式要与分组依据相同。init中已经初始化完毕一个哈希表。在next中依次吐出
  auto curr_schema = GetOutputSchema();
  // 遍历完整个哈希表以后空表要特殊处理
  if (aht_iterator_ == end_) {
    // 特殊情况，当为空表，且想获得统计信息时，只有countstar返回0，其他情况返回无效null
    // 在空表上执行聚合时，CountStarAggregate 应返回零，而所有其他聚合类型应返回 integer_null
    if (aht_iterator_ == aht_.Begin() && empty_output_) {
      // 空表只输出一次。用符号额外标记是否输出过
      if (!plan_->GetGroupBys().empty()) {
        // 检查是否存在分组依据
        return false;
      }
      auto value = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(value.aggregates_, &curr_schema);
      empty_output_ = false;
      return true;
    }
    // 结束
    return false;
  }
  auto key = aht_iterator_.Key();
  auto value = aht_iterator_.Val();
  std::vector<Value> values;
  values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
  values.insert(values.end(), value.aggregates_.begin(), value.aggregates_.end());
  *tuple = Tuple(values, &curr_schema);
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
