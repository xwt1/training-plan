//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan) {
          child_executor_ = std::move(child_executor);
    }

void InsertExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    auto table_oid = plan_->TableOid();
    this->table_info_ = catalog->GetTable(table_oid);
    auto table_name = this->table_info_->name_;
    this->index_info_ = catalog->GetTableIndexes(table_name);
    this->has_out_ = false;
    child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    int sum = 0;
    // 准备tuple和rid接收子算子的tuple
    Tuple temp_tuple;
    RID temp_rid;
    while(this->child_executor_->Next(&temp_tuple, &temp_rid)){
        // 插入一个Tuple,首先准备一个空的元数据
        TupleMeta tuple_meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
        // 插入tuple
        auto insert_rid = table_info_->table_->InsertTuple(tuple_meta, temp_tuple);
        if(insert_rid != std::nullopt){
            ++sum;
            for(auto &index_info : this->index_info_){
                // std::cout<<"索引名: "<<index_info->name_<<std::endl;
                auto insert_tuple = this->table_info_->table_->GetTuple(*insert_rid).second;
                // std::cout<<"要插入的tuple: "<<insert_tuple.GetValue()<<
                // 这里理论上应该只能获取到B_PLUS_TREE的index,其他都没有实现
                // auto index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
                // index_info->
                // 首先拿到插入的这条tuple的key,可以看到在tuple.h中有一个KeyFromTuple的方法专门用于计算并获取一个tuple的key
                auto tuple_key = insert_tuple.KeyFromTuple(this->table_info_->schema_, 
                    *(index_info->index_->GetKeySchema()), 
                    index_info->index_->GetKeyAttrs());
                // 更新索引
                // Value v1 = tuple_key.GetValue(index->GetKeySchema(),0);
                // std::cout<<"本次插入的tuple的key值为: "<<v1.ToString()<<std::endl;
                index_info->index_->InsertEntry(tuple_key,*insert_rid,exec_ctx_->GetTransaction());
                // std::cout<<is_succ<<std::endl;
            }
        }else{
            std::cout<<"过于巨大,可能出错了"<<std::endl;
        }
    }
    bool now_has_out = this->has_out_;
    this->has_out_ = true;

    std::vector<Column> columns{};
    columns.emplace_back(Column{"num_", TypeId::INTEGER});
    Schema sch{columns};
    Value v(INTEGER,sum);
    std::vector<Value> value = {v};
    *tuple = Tuple(value, &sch);

    return !now_has_out; 
}

}  // namespace bustub
