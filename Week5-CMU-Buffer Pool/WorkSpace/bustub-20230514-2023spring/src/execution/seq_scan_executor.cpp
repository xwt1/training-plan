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
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // this->plan_ = plan;
  // this->Init();
}

SeqScanExecutor::~SeqScanExecutor() {
  if (this->it_ != nullptr) {
    delete this->it_;
    this->it_ = nullptr;
  }
}

void SeqScanExecutor::Init() {
  auto oid = plan_->GetTableOid();
  if (exec_ctx_->IsDelete()) {
    AcquireTableLock(LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  } else {
    auto now_txn = exec_ctx_->GetTransaction();
    if (!(now_txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED)) {
      if (!(now_txn->GetExclusiveTableLockSet()->count(oid) > 0)) {
        if (!(now_txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0)) {
          AcquireTableLock(LockManager::LockMode::INTENTION_SHARED, oid);
        }
      }
    }
  }

  auto cata_log = exec_ctx_->GetCatalog();
  auto table_info = cata_log->GetTable(plan_->GetTableOid());
  auto get_table = table_info->table_.get();

  this->it_ = new TableIterator(get_table->MakeEagerIterator());
}

void SeqScanExecutor::AcquireTableLock(const LockManager::LockMode &request_lock_mode, const table_oid_t &oid) {
  try {
    bool res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), request_lock_mode, oid);
    if (!res) {
      switch (request_lock_mode) {
        case LockManager::LockMode::EXCLUSIVE: {
          throw ExecutionException("seq_scan_executor acquire X Table lock fail");
          break;
        }
        case LockManager::LockMode::INTENTION_EXCLUSIVE: {
          throw ExecutionException("seq_scan_executor acquire IX Table lock fail");
          break;
        }
        case LockManager::LockMode::SHARED: {
          throw ExecutionException("seq_scan_executor acquire S Table lock fail");
          break;
        }
        case LockManager::LockMode::INTENTION_SHARED: {
          throw ExecutionException("seq_scan_executor acquire IS Table lock fail");
          break;
        }
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE: {
          throw ExecutionException("seq_scan_executor acquire SIX Table lock fail");
          break;
        }
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
    // std::cout<<e.GetInfo()<<std::endl;
  }
}

void SeqScanExecutor::ReleaseTableLock(const table_oid_t &oid) {
  try {
    bool res = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
    if (!res) {
      throw ExecutionException("seq_scan_executor unlock fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
  }
}

void SeqScanExecutor::AcquireRowLock(const LockManager::LockMode &request_lock_mode, const table_oid_t &oid,
                                     const RID &rid) {
  try {
    bool res = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), request_lock_mode, oid, rid);
    if (!res) {
      switch (request_lock_mode) {
        case LockManager::LockMode::EXCLUSIVE: {
          throw ExecutionException("seq_scan_executor acquire X ROW lock fail");
          break;
        }
        case LockManager::LockMode::SHARED: {
          throw ExecutionException("seq_scan_executor acquire S ROW lock fail");
          break;
        }
        case LockManager::LockMode::INTENTION_SHARED:
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
          break;
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
    // std::cout<<e.GetInfo()<<std::endl;
  }
}

void SeqScanExecutor::ReleaseRowLock(const table_oid_t &oid, const RID &rid, bool force) {
  try {
    bool res = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
    if (!res) {
      throw ExecutionException("seq_scan_executor unlock Row fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == nullptr) {
    return false;
  }
  while (true) {
    if (this->it_->IsEnd()) {
      auto now_txn = exec_ctx_->GetTransaction();
      if (now_txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        if (now_txn->GetIntentionExclusiveTableLockSet()->count(plan_->GetTableOid()) == 0) {
          ReleaseTableLock(plan_->GetTableOid());
        }
      }
      delete this->it_;
      this->it_ = nullptr;
      return false;
    }
    *tuple = this->it_->GetTuple().second;
    *rid = tuple->GetRid();

    if (exec_ctx_->IsDelete()) {
      AcquireRowLock(LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), tuple->GetRid());
    } else {
      auto now_txn = exec_ctx_->GetTransaction();
      if (!(now_txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED)) {
        if (now_txn->GetExclusiveRowLockSet()->count(plan_->GetTableOid()) == 0 ||
            now_txn->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid()) == 0) {
          AcquireRowLock(LockManager::LockMode::SHARED, plan_->GetTableOid(), tuple->GetRid());
        }
      }
    }

    auto tuple_meta = this->it_->GetTuple().first;
    // 检查两个点,如果其中某个点没有满足,就要强制unlock(或的关系)
    //     1. 这个tuple已经被标记删除
    //     2. 假如上层结点是一个filter过滤结点,而这个tuple不满足过滤的条件
    if (tuple_meta.is_deleted_ ||
        (plan_->filter_predicate_ != nullptr &&
         plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
                 .CompareEquals(bustub::ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) {
      // 必须判断是否为RUC,因为RUC不能拿读锁,而这个地方如果要解读锁,且事务隔离级别是RUC,那么解的话
      // 由于之前没有拿过读锁,故会在LOCKROW中将事务状态置为ABORTED,但是根据提交来看这是一种需要特判
      // 的情况,因为BUSTUB认为如果一个事务如果是RUC的话,在拿完读锁后状态变为ABORTED是非法的
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->count(plan_->GetTableOid()) == 0 ||
            exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid()) ==
                0) {
          ReleaseRowLock(plan_->GetTableOid(), tuple->GetRid(), true);
        }
      }
      ++(*(this->it_));
      continue;
    }
    *tuple = this->it_->GetTuple().second;
    *rid = tuple->GetRid();
    auto txn = exec_ctx_->GetTransaction();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->count(plan_->GetTableOid()) == 0 ||
          exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid()) == 0) {
        ReleaseRowLock(plan_->GetTableOid(), tuple->GetRid(), false);
      }
    }

    ++(*(this->it_));
    return true;
  }
}

}  // namespace bustub
