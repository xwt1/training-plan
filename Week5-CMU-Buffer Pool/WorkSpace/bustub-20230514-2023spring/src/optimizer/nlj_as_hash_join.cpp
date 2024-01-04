#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::DealWithChildren(const std::shared_ptr<HashJoinPlanNode> &node) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  // std::cout<<node->GetChildren().size()<<std::endl;
  // int num = 1;
  for (const auto &child : node->GetChildren()) {
    // if (child->GetType() == PlanType::NestedLoopJoin) {
    //   // child 指向 NestedLoopJoinPlanNode
    //   std::cout<<"第"<<num++<<"个孩子是njs"<<std::endl;
    // }
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  AbstractPlanNodeRef ret = node->CloneWithChildren(std::move(children));
  return ret;
}

auto Optimizer::SplitEquExpr(const bustub::AbstractExpression *expr, std::vector<AbstractExpressionRef> &left_exprs,
                             std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  auto com_expr = dynamic_cast<const ComparisonExpression *>(expr);
  if (com_expr != nullptr) {
    // 保证是比较表达式
    if (com_expr->comp_type_ == ComparisonType::Equal) {
      // 保证是等号
      auto compare_first_column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr->children_[0]);
      auto compare_second_column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr->children_[1]);

      if (compare_first_column_expr != nullptr && compare_second_column_expr != nullptr) {
        // 只有当左右两个孩子都是ColumnValueExpression才能继续进行
        if (compare_first_column_expr->GetTupleIdx() == 0 && compare_second_column_expr->GetTupleIdx() == 1) {
          // 说明第一个列取值表达式取得是左表，而第二个列取值表达式取得得是右表
          left_exprs.push_back(compare_first_column_expr);
          right_exprs.push_back(compare_second_column_expr);
        } else if (compare_first_column_expr->GetTupleIdx() == 1 && compare_second_column_expr->GetTupleIdx() == 0) {
          // 说明第一个列取值表达式取得是右表，而第二个列取值表达式取得得是左表
          left_exprs.push_back(compare_second_column_expr);
          right_exprs.push_back(compare_first_column_expr);
        }
        return true;
      }
      // const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
      // if (left_expr != nullptr) {
      //   const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
      //   if (right_expr != nullptr) {
      //     // Ensure both exprs have tuple_id == 0
      //     auto left_expr_tuple_0 =
      //         std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
      //     auto right_expr_tuple_0 =
      //         std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());

      //     if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
      //       left_exprs.push_back(left_expr_tuple_0);
      //       right_exprs.push_back(right_expr_tuple_0);
      //     } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
      //       left_exprs.push_back(right_expr_tuple_0);
      //       right_exprs.push_back(left_expr_tuple_0);
      //     }
      //     return true;
      //   }
      // }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  // 采取自顶向下的方法,凸显一个叛逆233
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    auto nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    std::vector<bustub::AbstractExpressionRef> left_exprs{};
    std::vector<bustub::AbstractExpressionRef> right_exprs{};
    auto predicate = nested_loop_join_plan.predicate_.get();
    if (this->SplitEquExpr(predicate, left_exprs, right_exprs)) {
      // 这里情况是只有一个等值表达式,可以进行优化
      if (left_exprs.size() == 1 && right_exprs.size() == 1) {
        auto optimizing_plan = std::make_shared<HashJoinPlanNode>(
            nested_loop_join_plan.output_schema_, nested_loop_join_plan.GetLeftPlan(),
            nested_loop_join_plan.GetRightPlan(), left_exprs, right_exprs, nested_loop_join_plan.GetJoinType());
        // std::cout<<"hash_join"<<std::endl;
        auto optimized_plan = this->DealWithChildren(optimizing_plan);
        return optimized_plan;
      }
    } else {
      // 这里要先判一下是否是logical_expression
      // 理论上这里可以写个递归判断无限量条件的等值连接
      //  思路：直接写一个check函数，判断当前结点是否为LogicExpression, 是则接着check孩子结点。
      //  终止条件:如果有孩子结点不为LogicExpression，若为ColumnValueExpression,则返回true,同时如同上面的SplitEquExpr函数一样收集
      //  left_exprs与right_exprs;若不为ColumnValueExpression,返回false说明无法应用优化。根据返回值判断能否应用优化。
      const auto *expr = dynamic_cast<const LogicExpression *>(nested_loop_join_plan.Predicate().get());
      if (expr != nullptr) {
        auto left_expr = expr->children_[0].get();
        auto right_expr = expr->children_[1].get();
        if (this->SplitEquExpr(left_expr, left_exprs, right_exprs) &&
            this->SplitEquExpr(right_expr, left_exprs, right_exprs)) {
          // 这时候保证left_expr与right_expr都是等号比较表达式
          if (left_exprs.size() == 2 && right_exprs.size() == 2) {
            auto optimizing_plan = std::make_shared<HashJoinPlanNode>(
                nested_loop_join_plan.output_schema_, nested_loop_join_plan.GetLeftPlan(),
                nested_loop_join_plan.GetRightPlan(), left_exprs, right_exprs, nested_loop_join_plan.GetJoinType());
            auto optimized_plan = this->DealWithChildren(optimizing_plan);
            return optimized_plan;
          }
        }
      }
    }
  }
  // 说明不符合本条优化规则
  return plan;
}

}  // namespace bustub
