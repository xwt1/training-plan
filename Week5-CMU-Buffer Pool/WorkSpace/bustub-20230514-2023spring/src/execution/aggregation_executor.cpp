//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
        plan_(plan),
        child_(std::move(child)),
        aht_(SimpleAggregationHashTable(this->plan_->GetAggregates(),this->plan_->GetAggregateTypes())),
        aht_iterator_(aht_.Begin()){
        // this->child_ = std::move(child);
        // this->aht_ = std::move(SimpleAggregationHashTable(this->plan_->GetAggregates(),this->plan_->GetAggregateTypes()));
    }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  aht_.Clear();
  while (child_->Next(&tuple, &rid)) {
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
  this->has_out_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(aht_.IsEmpty()){
    // std::cout<<"哈希表为空"<<std::endl;
    // 假如这时候发现表为空,则要讨论是否有group by clause(子句)
    if(plan_->GetGroupBys().empty()){
      // std::cout<<"group by为空"<<std::endl;
      // 如果没有group by clause(子句),则需要查看是否特殊输出过一个tuple,如果没有则输出
      if(!has_out_){
        std::vector<Value> values;
        auto aggregate_types = plan_->GetAggregateTypes();
        for (size_t i = 0; i < plan_->GetAggregates().size(); i++) {
          if (aggregate_types[i] == AggregationType::CountStarAggregate) {
            values.push_back(ValueFactory::GetIntegerValue(0));
          } else {
            values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          }
        }
        *tuple = Tuple(values, &GetOutputSchema());
        has_out_ = true;
        return true;
      }
    }
    return false;
  }
  if (aht_iterator_ != aht_.End()) {
    std::vector<bustub::Value> vals{};
    for (auto &group_by_val : aht_iterator_.Key().group_bys_) {
      vals.push_back(group_by_val);
    }
    for (auto &aggregate_value : aht_iterator_.Val().aggregates_) {
      vals.push_back(aggregate_value);
    }
    Tuple result{vals, &plan_->OutputSchema()};
    *tuple = result;
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
