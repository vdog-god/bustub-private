//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  // value值合并函数，将两个表的tuple组合成新的tuple
  void MergeValueFromTuple(std::vector<Value> &value, bool right_null) const {
    auto left_column_count = left_schema_.GetColumnCount();
    auto right_column_count = plan_->InnerTableSchema().GetColumnCount();
    for (unsigned int i = 0; i < left_column_count; i++) {
      value.push_back(outer_tuple_.GetValue(&left_schema_, i));
    }
    for (unsigned int i = 0; i < right_column_count; i++) {
      if (!right_null) {
        value.push_back(inner_tuple_.GetValue(&plan_->InnerTableSchema(), i));
      } else {
        value.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
    }
  }

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  IndexInfo *inner_index_{nullptr};
  TableInfo *inner_table_ptr_{nullptr};
  Schema *index_schema_;
  Schema left_schema_;

  Tuple inner_tuple_{};
  Tuple outer_tuple_{};  // sql语句中的左表在算子中为外表，由child_executor提供next
};
}  // namespace bustub
