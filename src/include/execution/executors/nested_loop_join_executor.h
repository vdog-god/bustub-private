//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  // value值合并函数，将两个表的tuple组合成新的tuple
  void MergeValueFromTuple(std::vector<Value> &value, bool right_isnull) const {
    auto left_column_count = left_executor_->GetOutputSchema().GetColumnCount();
    auto right_column_count = right_executor_->GetOutputSchema().GetColumnCount();
    for (unsigned int i = 0; i < left_column_count; i++) {
      value.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (unsigned int i = 0; i < right_column_count; i++) {
      if (!right_isnull) {
        value.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
      } else {
        value.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
    }
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  Tuple left_tuple_{};
  Tuple right_tuple_{};
  bool next_ltuple_existent_{false};
  bool next_rtuple_existent_{false};
  RID left_rid_{};
  RID right_rid_{};
  // 左连接右表有无匹配项
  bool right_found_{false};
};

}  // namespace bustub
