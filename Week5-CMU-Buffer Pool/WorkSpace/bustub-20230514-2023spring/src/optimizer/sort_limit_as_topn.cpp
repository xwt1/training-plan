#include "include/execution/plans/limit_plan.h"
#include "include/execution/plans/sort_plan.h"
#include "include/execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children{};
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_pan = plan->CloneWithChildren(std::move(children));
  if (optimized_pan->GetType() == PlanType::Limit) {
    if (optimized_pan->children_.size() == 1 && optimized_pan->children_[0]->GetType() == PlanType::Sort) {
      const auto &limited_plan_node = dynamic_cast<const LimitPlanNode &>(*optimized_pan);
      const auto &sorted_plan_node = dynamic_cast<const SortPlanNode &>(*optimized_pan->children_[0]);
      return std::make_shared<TopNPlanNode>(std::make_shared<Schema>(limited_plan_node.OutputSchema()),
                                            sorted_plan_node.children_[0], sorted_plan_node.GetOrderBy(),
                                            limited_plan_node.GetLimit());
    }
  }
  return optimized_pan;
}

}  // namespace bustub
