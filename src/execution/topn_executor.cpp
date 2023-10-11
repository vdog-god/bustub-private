#include "execution/executors/topn_executor.h"
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  sorted_tuple_.clear();

  auto orderby_schema = GetOutputSchema();
  // lambda表达式接受两个tuple，用来比较
  // 比较时要用到对应的orderby字段，可以从plan中AbstractExpressionRef获取
  // 同时这个表达式可以提供evalute函数，针对当前模式，输出用来比较的字段
  // 排序字段可以有多个，按先后顺序比较。第一个不相等，直接得到结果；相等，则比较第二个。不会出现所有字段全部相等的情况。
  auto cmp = [&](Tuple &lhs, Tuple &rhs) -> bool {
    for (const auto &[order_type, expr] : plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&lhs, orderby_schema);
      auto right_value = expr->Evaluate(&rhs, orderby_schema);

      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      // 返回false时不改变原来顺序 返回true时与原来顺序相反
      // 对于升序，要求lhs<rhs。当lessthan，则返回false
      // 对于降序，则相反
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return left_value.CompareLessThan(right_value) == CmpBool::CmpTrue;
      }
      BUSTUB_ASSERT(order_type == OrderByType::DESC, "一定是降序，不应该为无效的排序方式");
      return left_value.CompareLessThan(right_value) != CmpBool::CmpTrue;
    }
    // 正常的话不会命中这个return
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    // 将tuple加入队列，大根堆会自动排序
    pq.push(tuple);
    // 当队列个数大于要输出的N时，将末尾元素弹出
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    sorted_tuple_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuple_.empty()) {
    return false;
  }
  *tuple = sorted_tuple_.back();
  sorted_tuple_.pop_back();
  return true;
}

}  // namespace bustub
