//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      iter_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(this->index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  iter_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(this->index_info_->index_.get())->GetBeginIterator();
  // auto catalog = exec_ctx_->GetCatalog();
  // this->index_info_ = catalog->GetIndex(this->plan_->index_oid_);
  // this->table_info_ = catalog->GetTable(this->index_info_->table_name_);
  // // this->iter_ = std::move(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn
  // *>(this->index_info_->index_.get())->GetBeginIterator()); this->has_out_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // auto iter =
  // 1.使用B+树的迭代器遍历每一个value(RID),利用这个rid去table堆里找对应的tuple
  if (this->index_info_ != nullptr) {
    while (!iter_.IsEnd()) {
      // std::cout<<"正在遍历"<<std::endl;
      auto rd = (*iter_).second;
      auto tp = table_info_->table_->GetTuple(rd);
      // if (tp.first.is_deleted_) {
      //     ++iter_;
      // } else {
      *tuple = tp.second;
      *rid = tuple->GetRid();
      ++iter_;
      return true;
      // }
    }
  }
  // 2.检测刚刚拿出来的tuple是否是deleted的，如果不是则放入tuple中
  return false;
}

}  // namespace bustub

// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // index_scan_executor.cpp
// //
// // Identification: src/execution/index_scan_executor.cpp
// //
// // Copyright (c) 2015-19, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//
// #include "execution/executors/index_scan_executor.h"

// namespace bustub {
// IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
//     : AbstractExecutor(exec_ctx),
//       plan_(plan),
//       info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)),
//       table_info_(exec_ctx->GetCatalog()->GetTable(info_->table_name_)),
//       iter_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(info_->index_.get())->GetBeginIterator()) {}

// void IndexScanExecutor::Init() {}

// auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   if (info_ != nullptr) {
//     while (!iter_.IsEnd()) {
//       auto rd = (*iter_).second;
//       auto tp = table_info_->table_->GetTuple(rd);
//       if (tp.first.is_deleted_) {
//         ++iter_;
//       } else {
//         *tuple = tp.second;
//         *rid = tuple->GetRid();
//         ++iter_;
//         return true;
//       }
//     }
//   }
//   return false;
// }

// }  // namespace bustub
