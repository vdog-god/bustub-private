#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // 对 plan tree 进行后序遍历，自底向上地适用规则，改写节点。遍历到某个节点时，通过 if
  // 语句来判断当前节点的类型是否符合我们要优化的类型，若符合则进行优化。

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // 上层节点为 Limit，下层节点为 Sort，不能反过来。同样，我们对 plan tree 进行后续遍历，在遇到 Limit
  // 时，判断其下层节点是否为 Sort，若为 Sort，则将这两个节点替换为一个 TopN。
  if (optimized_plan->GetType() == PlanType::Limit) {
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? Impossible!");
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() == PlanType::Sort) {
      // LIMIT + SORT combo match
      const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(),
                                            sort_plan.GetOrderBy(), limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
