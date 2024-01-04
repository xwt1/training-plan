//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->child_executor_ = std::move(child_executor);
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  this->table_info_ = catalog->GetTable(table_oid);
  auto table_name = this->table_info_->name_;
  this->index_info_ = catalog->GetTableIndexes(table_name);
  this->has_out_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 准备tuple和rid接收子算子的tuple
  Tuple temp_tuple;
  RID temp_rid;
  // 1.遍历子算子的每一个tuple,这个地方可以写循环也可以一次Next就拿一个tuple
  int update_num = 0;
  while (this->child_executor_->Next(&temp_tuple, &temp_rid)) {
    // 2.根据子算子的tuple找到对应要修改的tuple，修改其meta为删除的状态
    TupleMeta delete_meta{};
    delete_meta.delete_txn_id_ = delete_meta.insert_txn_id_ = INVALID_TXN_ID;
    delete_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(delete_meta, temp_rid);  // 修改table的meta

    // 3.插入新的tuple,新的tuple需要从plan中拿
    delete_meta.is_deleted_ = false;
    std::vector<bustub::Value> value_insert;
    for (const auto &expr : plan_->target_expressions_) {
      value_insert.emplace_back(expr->Evaluate(&temp_tuple, table_info_->schema_));
    }
    Tuple inserted_tuple{value_insert, &table_info_->schema_};
    auto inserted_rid = table_info_->table_->InsertTuple(delete_meta, inserted_tuple);  // 复用meta
    if (inserted_rid == std::nullopt) {
      std::cout << "插入过于巨大,可能出错了" << std::endl;
      continue;
    }
    // 4.更新索引
    for (auto &index_info : this->index_info_) {
      // 这里理论上应该只能获取到B_PLUS_TREE的index,其他都没有实现
      // auto index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
      // 首先拿到插入的这条tuple的key,可以看到在tuple.h中有一个KeyFromTuple的方法专门用于计算并获取一个tuple的key
      auto insert_tuple_key = inserted_tuple.KeyFromTuple(
          this->table_info_->schema_, *(index_info->index_->GetKeySchema()), index_info->index_->GetKeyAttrs());
      // 更新索引,包括删除原来的tuple的索引和插入新tuple的索引
      auto delete_tuple = this->table_info_->table_->GetTuple(temp_rid).second;
      auto delete_tuple_key = delete_tuple.KeyFromTuple(
          this->table_info_->schema_, *(index_info->index_->GetKeySchema()), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(delete_tuple_key, temp_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(insert_tuple_key, *inserted_rid, exec_ctx_->GetTransaction());
    }
    update_num++;
  }
  bool now_has_out = this->has_out_;
  this->has_out_ = true;

  // 5.如果更新,则返回更新的数量.
  std::vector<Column> columns{};
  columns.emplace_back(Column{"num_", TypeId::INTEGER});
  Schema sch{columns};
  std::vector<bustub::Value> values{};
  values.emplace_back(Value{TypeId::INTEGER, update_num});
  *tuple = Tuple{values, &sch};

  return !now_has_out;
}

}  // namespace bustub
