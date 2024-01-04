#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  this->child_executor_->Init();
  // auto catalog = this->exec_ctx_->GetCatalog();
  Tuple temp_tuple;
  RID temp_rid;
  while (this->child_executor_->Next(&temp_tuple, &temp_rid)) {
    sort_tuple_.emplace_back(temp_tuple);
  }
  std::sort(sort_tuple_.begin(), sort_tuple_.end(), [&](const Tuple &A, const Tuple &B) {
    for (const auto &order_by_item : this->plan_->GetOrderBy()) {
      Value a_value = order_by_item.second->Evaluate(&A, this->plan_->OutputSchema());
      auto b_value = order_by_item.second->Evaluate(&B, this->plan_->OutputSchema());
      switch (order_by_item.first) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC: {
          if (a_value.CompareLessThan(b_value) == CmpBool::CmpTrue) {
            return true;
          }
          if (a_value.CompareGreaterThan(b_value) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        }
        case OrderByType::DESC: {
          if (a_value.CompareLessThan(b_value) == CmpBool::CmpTrue) {
            return false;
          }
          if (a_value.CompareGreaterThan(b_value) == CmpBool::CmpTrue) {
            return true;
          }
          break;
        }
        default: {
          // BUSTUB_ASSERT(true,"OrderByType::INVALID happened!!");
        }
      }
    }
    return true;
  });
  std::reverse(sort_tuple_.begin(), sort_tuple_.end());
  // std::cout<<1<<std::endl;
  // std::cout<<"sort_tuple_.size(): "<<sort_tuple_.size()<<std::endl;
}

auto SortExecutor::Next([[maybe_unused]] Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  // std::cout<<2<<std::endl;
  if (!sort_tuple_.empty()) {
    // std::cout<<"wtf"<<std::endl;
    // std::cout<<"sort_tuple_.size(): "<<sort_tuple_.size()<<std::endl;
    decltype(sort_tuple_.size()) siz = sort_tuple_.size();
    auto temp_tuple = sort_tuple_[siz - 1];
    auto temp_rid = temp_tuple.GetRid();
    *tuple = temp_tuple;
    // *rid = tuple->GetRid();
    *rid = temp_rid;
    sort_tuple_.pop_back();
    // std::cout<<"sort_tuple_.size() after: "<<sort_tuple_.size()<<std::endl;
    return true;
  }
  return false;
}

}  // namespace bustub
