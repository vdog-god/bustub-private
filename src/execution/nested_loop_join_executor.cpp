//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 初始化非常重要，尤其要重置指针。
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_ = Tuple{};
  right_tuple_ = Tuple{};
  left_rid_ = RID{};
  right_rid_ = RID{};
  next_ltuple_existent_ = left_executor_->Next(&left_tuple_, &left_rid_);
  next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 左连接要考虑输出null，内连接要考虑输出相互匹配。

  if (!next_ltuple_existent_) {  // 对于内连接和左连接，左表空直接结束
    return false;
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (next_ltuple_existent_) {
      while (next_rtuple_existent_) {
        auto pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                          &right_tuple_, right_executor_->GetOutputSchema());

        if (!pred_value.GetAs<bool>()) {
          // GetAs()函数会将value转换成bool值。为true说明两者相匹配 此时将左右两表拼接后输出
          next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
        } else {
          std::vector<Value> value;
          MergeValueFromTuple(value, false);
          *tuple = Tuple(value, &plan_->OutputSchema());
          right_found_ = true;
          next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
          return true;
        }
      }
      // 循环结束后表明右表遍历完成，若右表没有相匹配的值 right_found为false，表明要为额外输出null
      // 还要让左表指向下一条值,右表迭代器初始化接着遍历
      if (!right_found_) {
        std::vector<Value> value;
        MergeValueFromTuple(value, true);
        *tuple = Tuple(value, &plan_->OutputSchema());

        right_found_ = false;
        next_ltuple_existent_ = left_executor_->Next(&left_tuple_, &left_rid_);
        right_executor_->Init();
        next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);

        return true;
      }
      //
      right_found_ = false;
      next_ltuple_existent_ = left_executor_->Next(&left_tuple_, &left_rid_);
      right_executor_->Init();
      next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
    }
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    while (next_ltuple_existent_) {
      while (next_rtuple_existent_) {
        auto pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                          &right_tuple_, right_executor_->GetOutputSchema());

        if (!pred_value.GetAs<bool>()) {
          // GetAs()函数会将value转换成bool值。为true说明两者相匹配 此时将左右两表拼接后输出
          next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
        } else {
          std::vector<Value> value;
          MergeValueFromTuple(value, false);
          *tuple = Tuple(value, &plan_->OutputSchema());
          next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
          return true;
        }
      }
      // 循环结束后表明右表遍历完成，此时要让左表指向下一条值,右表迭代器初始化接着遍历
      next_ltuple_existent_ = left_executor_->Next(&left_tuple_, &left_rid_);
      right_executor_->Init();
      next_rtuple_existent_ = right_executor_->Next(&right_tuple_, &right_rid_);
    }
    return false;
  }
  return false;
}

}  // namespace bustub
