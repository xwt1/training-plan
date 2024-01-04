#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  this->child_executor_->Init();
  Tuple temp_tuple;
  RID temp_rid;
  while (this->child_executor_->Next(&temp_tuple, &temp_rid)) {
    this->topn_tuple_.push({temp_tuple, &this->GetOutputSchema(), this->plan_->GetOrderBy()});
    if (this->topn_tuple_.size() > this->plan_->GetN()) {
      this->topn_tuple_.pop();
    }
  }
  while (!topn_tuple_.empty()) {
    this->res_.push_back(topn_tuple_.top());
    topn_tuple_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!this->res_.empty()) {
    auto nodes = this->res_.back();
    this->res_.pop_back();
    *tuple = nodes.tuple_;
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return topn_tuple_.size(); };

}  // namespace bustub
