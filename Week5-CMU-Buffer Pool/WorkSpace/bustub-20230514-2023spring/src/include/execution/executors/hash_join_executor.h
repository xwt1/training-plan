//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

// struct HashJoinKey {
//   std::vector<Value> predicate_cols_;

//   auto operator==(const HashJoinKey &other) const -> bool {
//     for (uint32_t i = 0; i < other.predicate_cols_.size(); i++) {
//       if (predicate_cols_[i].CompareEquals(other.predicate_cols_[i]) != CmpBool::CmpTrue) {
//         return false;
//       }
//     }
//     return true;
//   }
// };

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  class HashJoinTableIterator{
    public:
    HashJoinTableIterator()=default;
    explicit HashJoinTableIterator(std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > >::iterator table_iterator,
    const std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > >::iterator &table_iterator_end):
    table_iterator_(table_iterator){
      if(table_iterator_ != table_iterator_end){
        list_iterator_ = table_iterator->second.begin();
      }
    }
    // 在进行任何此迭代器操作之前,必须调用本函数判断是否到达末尾!
    auto IsEnd() -> bool {
      return (table_iterator_ == table_iterator_end_);
    }

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return table_iterator_->first; }

    /** @return The value of the iterator */
    auto Val() -> Tuple  { return *(*list_iterator_); }

    /** @return The iterator before it is incremented */
    auto operator++() -> HashJoinTableIterator & {
      if(list_iterator_ != table_iterator_->second.end()){
        list_iterator_++;
      }else{
        if(table_iterator_ != table_iterator_end_){
          table_iterator_++;
          if(table_iterator_ != table_iterator_end_){
            list_iterator_ = table_iterator_->second.begin();
          }
        }
      }
      return *this;
    }

    auto operator=(const HashJoinTableIterator &other) -> HashJoinTableIterator & = default;


    /** @return `true` if both iterators are identical */
    auto operator==(const HashJoinTableIterator &other) -> bool { 
      return (this->table_iterator_ == other.table_iterator_ && this->list_iterator_ == other.list_iterator_); 
    }

    /** @return `true` if both iterators are different */
    auto operator!=(const HashJoinTableIterator &other) -> bool { 
      return (this->table_iterator_ != other.table_iterator_ || this->list_iterator_ != other.list_iterator_); 
    }
    
    std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > >::iterator table_iterator_;
    std::list<std::unique_ptr<Tuple> >::iterator list_iterator_;
    std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > >::iterator table_iterator_end_;
  };


 private:
  void AddTupleToHashTable(const Tuple &tuple, std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > > &hash_table,
                          const std::vector<bustub::AbstractExpressionRef> &join_key_expressions,const Schema &son_schema);
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  // bustub::SimpleAggregationHashTable ht_;
  std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > > left_ht_;
  std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > > right_ht_;
  HashJoinTableIterator left_ht_it_;
  HashJoinTableIterator right_ht_it_;

  bool is_left_has_right_{false};
  // HashJoinTableIterator left_ht_end_;
  // HashJoinTableIterator right_ht_end_;
  // std::list<std::unique_ptr<Tuple> >::iterator left_ht_it_;
  // std::list<std::unique_ptr<Tuple> >::iterator right_ht_it_;
};

}  // namespace bustub

