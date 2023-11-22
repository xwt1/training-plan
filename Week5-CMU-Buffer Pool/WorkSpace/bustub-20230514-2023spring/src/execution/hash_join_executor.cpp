//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_child)),
    right_executor_(std::move(right_child))
     {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  std::cout<<1<<std::endl;
}

void HashJoinExecutor::Init() {
  std::cout<<1<<std::endl;
  this->left_executor_->Init();
  this->right_executor_->Init();
  this->left_ht_.clear();
  this->right_ht_.clear();
  // 1.首先要对左节点建立哈希表,将左节点的对应等值连接条件的列的值作为key,tuple本身作为value存储起来
  Tuple temp_tuple;
  RID temp_rid;
  while(this->left_executor_->Next(&temp_tuple,&temp_rid)){
    this->AddTupleToHashTable(temp_tuple,this->left_ht_,this->plan_->LeftJoinKeyExpressions(),this->left_executor_->GetOutputSchema());
  }
  while(this->right_executor_->Next(&temp_tuple,&temp_rid)){
    this->AddTupleToHashTable(temp_tuple,this->right_ht_,this->plan_->RightJoinKeyExpressions(),this->right_executor_->GetOutputSchema());
  }
  this->left_ht_it_ = HashJoinTableIterator(left_ht_.begin(),left_ht_.end());
  this->right_ht_it_ = HashJoinTableIterator(right_ht_.begin(),right_ht_.end());
  this->is_left_has_right_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  switch (this->plan_->GetJoinType()){
    case JoinType::INNER:{
      // 内连接
      while(!left_ht_it_.IsEnd()){
        bool is_out = false;
        while(!right_ht_it_.IsEnd()){
          auto left_tuple = left_ht_it_.Val();
          auto right_tuple = right_ht_it_.Val(); 
          // 现在需要根据谓词条件将左右两个tuple拼接在一起。
          std::vector<Value> value;
          uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
          uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
          for(uint32_t i = 0 ;i< left_siz;i++){
            value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(),i));
          }
          for(uint32_t i = 0 ;i< right_siz;i++){
            value.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(),i));
          }
          *tuple = Tuple{value,&this->plan_->OutputSchema()};
          ++this->right_ht_it_;
          is_out = true;
          break;
        }
        if(!is_out){
          // 此时需要使得左哈希表迭代器++，并重置右哈希表的迭代器
          ++this->left_ht_it_;
          this->right_ht_it_ = HashJoinTableIterator(right_ht_.begin(),right_ht_.end());
        }else{
          return true;
        }
      }
      break;
    }

    case JoinType::LEFT:{
      // 左外连接
      while(!left_ht_it_.IsEnd()){
        bool is_out = false;
        while(!right_ht_it_.IsEnd()){
          auto left_tuple = left_ht_it_.Val();
          auto right_tuple = right_ht_it_.Val(); 
          // 现在需要根据谓词条件将左右两个tuple拼接在一起。
          std::vector<Value> value;
          uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
          uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
          for(uint32_t i = 0 ;i< left_siz;i++){
            value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(),i));
          }
          for(uint32_t i = 0 ;i< right_siz;i++){
            value.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(),i));
          }
          *tuple = Tuple{value,&this->plan_->OutputSchema()};
          ++this->right_ht_it_;
          this->is_left_has_right_ = true;
          is_out = true;
          break;
        }
        if(!is_out){
          // 此时需要使得左哈希表迭代器++，并重置右哈希表的迭代器
          // 首先特殊处理左连接
          if(!this->is_left_has_right_){
            auto left_tuple = left_ht_it_.Val();
            std::vector<Value> value;
            uint32_t left_siz = left_executor_->GetOutputSchema().GetColumnCount();
            uint32_t right_siz = right_executor_->GetOutputSchema().GetColumnCount();
            for(uint32_t i = 0 ;i< left_siz;i++){
              value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(),i));
            }
            for(uint32_t i = 0 ;i< right_siz;i++){
              value.emplace_back(ValueFactory::GetNullValueByType( right_executor_->GetOutputSchema().GetColumn(i).GetType()));
            }
            *tuple = Tuple{value,&this->plan_->OutputSchema()};
            ++this->left_ht_it_;
            this->is_left_has_right_ = false;
            this->right_ht_it_ = HashJoinTableIterator(right_ht_.begin(),right_ht_.end());
            return true;
          }
          ++this->left_ht_it_;
          this->is_left_has_right_ = false;
          this->right_ht_it_ = HashJoinTableIterator(right_ht_.begin(),right_ht_.end());
        }else{
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

void HashJoinExecutor::AddTupleToHashTable(const Tuple &tuple, std::unordered_map<AggregateKey, std::list<std::unique_ptr<Tuple> > > &hash_table,
const std::vector<bustub::AbstractExpressionRef> &join_key_expressions,const Schema &son_schema){
  AggregateKey v{};
  // v.reserve(left_expr.size());
  for(auto &expr : join_key_expressions){
    v.group_bys_.emplace_back(expr->Evaluate(&tuple, son_schema));
  }
  std::list<std::unique_ptr<Tuple> > l;
  if(!hash_table[v].empty()){
    l.push_back(std::make_unique<Tuple>(tuple));
    hash_table[v] = std::move(l);
  }else{
    // for(auto &item: hash_table[v]){
    //   l.push_back(std::move(item));
    //   hash_table[v].pop_front();
    // }
    // 上面注释的与下面这句话等价(大概)
    l = std::move(hash_table[v]);
    l.push_back(std::make_unique<Tuple>(tuple));
    hash_table[v] = std::move(l);
  }                       
}

}  // namespace bustub
