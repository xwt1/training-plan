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
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  this->left_executor_->Init();
  this->right_executor_->Init();
  // this->has_left_ = false;
  this->not_done_ = left_executor_->Next(&this->now_left_tuple_, &this->now_left_rid_);
  this->is_left_has_right_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (this->plan_->GetJoinType()) {
    case JoinType::INNER: {
      // 内连接
      while (true) {
        // 退出这个循环只有两种情况:第一种是左节点已经没有新的tuple了,第二种是连接产生了一个tuple
        if (!this->not_done_) {
          return false;
          // break;
        }
        Tuple temp_right_tuple;
        RID temp_right_rid;
        bool is_out = false;
        while (this->right_executor_->Next(&temp_right_tuple, &temp_right_rid)) {
          // std::cout<<"this->right_executor_"<<std::endl;
          auto nest_loop_predicate_expr = this->plan_->predicate_;
          // 判断一下谓词是否相等
          Value judge_predicate =
              nest_loop_predicate_expr->EvaluateJoin(&this->now_left_tuple_, left_executor_->GetOutputSchema(),
                                                     &temp_right_tuple, right_executor_->GetOutputSchema());
          if (!judge_predicate.IsNull() && judge_predicate.GetAs<bool>()) {
            // 说明这俩的谓词相等,符合内连接的条件,拼接后输出
            std::vector<Value> value;
            // 现在需要根据谓词条件将左右两个tuple拼接在一起。
            uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
            uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
            for (uint32_t i = 0; i < left_siz; i++) {
              value.emplace_back(this->now_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_siz; i++) {
              value.emplace_back(temp_right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }
            *tuple = Tuple{value, &this->plan_->OutputSchema()};
            is_out = true;
            break;
          }
        }
        // std::cout<<std::endl;
        if (!is_out) {
          // 这时候右节点已经取完了,需要重新从左节点开始匹配,同时初始化右结点
          this->not_done_ = this->left_executor_->Next(&this->now_left_tuple_, &this->now_left_rid_);
          if (this->not_done_) {
            this->right_executor_->Init();
          }
        } else {
          // 说明匹配到了,返回true
          return true;
        }
      }
      break;
    }

    case JoinType::LEFT: {
      // 左外连接
      while (true) {
        // 退出这个循环只有两种情况:第一种是左节点已经没有新的tuple了,第二种是连接产生了一个tuple
        if (!this->not_done_) {
          return false;
          // break;
        }
        Tuple temp_right_tuple;
        RID temp_right_rid;
        bool is_out = false;
        while (this->right_executor_->Next(&temp_right_tuple, &temp_right_rid)) {
          // 判断一下右子算子结点是否还有
          auto nest_loop_predicate_expr = this->plan_->predicate_;
          // 判断一下谓词是否相等
          Value judge_predicate =
              nest_loop_predicate_expr->EvaluateJoin(&this->now_left_tuple_, left_executor_->GetOutputSchema(),
                                                     &temp_right_tuple, right_executor_->GetOutputSchema());
          if (!judge_predicate.IsNull() && judge_predicate.GetAs<bool>()) {
            // 说明这俩的谓词相等,符合内连接的条件,拼接后输出
            std::vector<Value> value;
            // 现在需要根据谓词条件将左右两个tuple拼接在一起。
            uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
            uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
            for (uint32_t i = 0; i < left_siz; i++) {
              value.emplace_back(this->now_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_siz; i++) {
              value.emplace_back(temp_right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }
            *tuple = Tuple{value, &this->plan_->OutputSchema()};
            this->is_left_has_right_ = true;
            is_out = true;
            break;
          }
        }
        if (!is_out) {
          // 这时候右节点已经取完了,首先要检查is_left_has_right_的值
          if (!this->is_left_has_right_) {
            std::vector<Value> value;
            // 现在需要根据谓词条件将左右两个tuple拼接在一起。
            uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
            uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
            for (uint32_t i = 0; i < left_siz; i++) {
              value.emplace_back(this->now_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_siz; i++) {
              // value.emplace_back(ValueFactory::GetNullValueByType());
              value.emplace_back(
                  ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
            }
            *tuple = Tuple{value, &this->plan_->OutputSchema()};
            this->not_done_ = this->left_executor_->Next(&this->now_left_tuple_, &this->now_left_rid_);
            this->is_left_has_right_ = false;
            if (this->not_done_) {
              this->right_executor_->Init();
            }
            return true;
          }
          // 再重新从左节点开始匹配,同时初始化右结点
          this->not_done_ = this->left_executor_->Next(&this->now_left_tuple_, &this->now_left_rid_);
          this->is_left_has_right_ = false;
          if (this->not_done_) {
            this->right_executor_->Init();
          }
        } else {
          // 说明匹配到了,返回true
          return true;
        }
      }
      break;
    }

    case JoinType::INVALID:
    case JoinType::RIGHT:
    case JoinType::OUTER:
      break;
  }
  // 统一说明找不到,return false
  return false;
}

}  // namespace bustub
