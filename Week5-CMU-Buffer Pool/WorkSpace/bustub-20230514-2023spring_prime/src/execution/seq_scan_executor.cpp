//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {
    // this->plan_ = plan;
    // this->Init();
}

SeqScanExecutor::~SeqScanExecutor(){
    if(this->it_ != nullptr){
        delete this->it_;
        this->it_ = nullptr;
    }
}


void SeqScanExecutor::Init() { 
    auto cata_log = exec_ctx_->GetCatalog();
    auto table_info = cata_log->GetTable(plan_->GetTableOid());
    auto get_table = table_info->table_.get();
    this->it_ = new TableIterator(get_table->MakeIterator());
}

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
//     if(this->it_ == nullptr){
//         return false;
//     }
//     if(this->it_->IsEnd()){
//         delete this->it_;
//         this->it_ = nullptr;
//         return false;
//     }
//     *tuple = this->it_->GetTuple().second;
//     *rid = tuple->GetRid();
//     ++(*(this->it_));
//     return true; 
// }
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(this->it_ == nullptr){
        return false;
    }
    while(true){
        if(this->it_->IsEnd()){
            delete this->it_;
            this->it_ = nullptr;
            return false;
        }
        auto tuple_meta = this->it_->GetTuple().first;
        // std::cout<<tuple_meta.is_deleted_<<std::endl;
        if(!tuple_meta.is_deleted_){
            *tuple = this->it_->GetTuple().second;
            *rid = tuple->GetRid();
            ++(*(this->it_));
            return true; 
        }
        ++(*(this->it_));
    }
}

}  // namespace bustub
