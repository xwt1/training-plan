//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class TopNNode {
 public:
  friend class TopNExecutor;
  TopNNode() = default;
  TopNNode(Tuple tuple, const Schema *const sc,
           const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys)
      : tuple_(std::move(tuple)) {
    for (const auto &order_by_item : order_bys) {
      Value v = order_by_item.second->Evaluate(&this->tuple_, *sc);
      this->vec_.emplace_back(order_by_item.first, v);
    }
  }
  auto operator<(const TopNNode &A) const -> bool {
    // 重载小于号,构建大根堆
    BUSTUB_ASSERT(A.vec_.size() == this->vec_.size(), "两个node类型不一样,无法比较");
    decltype(this->vec_.size()) siz = this->vec_.size();
    for (decltype(this->vec_.size()) i = 0; i < siz; i++) {
      BUSTUB_ASSERT(A.vec_[i].first == this->vec_[i].first, "两个node的OrderByType不一样,无法比较");
      switch (this->vec_[i].first) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC: {
          if (this->vec_[i].second.CompareLessThan(A.vec_[i].second) == CmpBool::CmpTrue) {
            return true;
          }
          if (this->vec_[i].second.CompareGreaterThan(A.vec_[i].second) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        }
        case OrderByType::DESC: {
          if (this->vec_[i].second.CompareLessThan(A.vec_[i].second) == CmpBool::CmpTrue) {
            return false;
          }
          if (this->vec_[i].second.CompareGreaterThan(A.vec_[i].second) == CmpBool::CmpTrue) {
            return true;
          }
          break;
        }
        default: {
          BUSTUB_ASSERT(true, "OrderByType::INVALID happened!!");
        }
      }
    }
    return false;
  }

 private:
  Tuple tuple_;
  std::vector<std::pair<bustub::OrderByType, bustub::Value>> vec_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // HashJoinExecutor::HashJoinTableIterator it1_;
  std::priority_queue<TopNNode> topn_tuple_;
  std::vector<TopNNode> res_;
};
}  // namespace bustub
