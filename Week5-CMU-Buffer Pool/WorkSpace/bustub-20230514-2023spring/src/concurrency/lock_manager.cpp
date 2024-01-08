//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

// #define DEBUG true

void LockManager::TransactionIsolationLevelCheck(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    BUSTUB_ASSERT(1, "txn->GetState() == TransactionState::COMMITTED或者TransactionState::ABORTED");
    throw("error");
  }
  // REPEATABLE_READ:
  //  The transaction is required to take all locks.
  //  All locks are allowed in the GROWING state
  //  No locks are allowed in the SHRINKING state

  // READ_COMMITTED:
  //   The transaction is required to take all locks.
  //   All locks are allowed in the GROWING state
  //   Only IS, S locks are allowed in the SHRINKING state

  // READ_UNCOMMITTED:
  //   The transaction is required to take only IX, X locks.
  //   X, IX locks are allowed in the GROWING state.
  //   S, IS, SIX locks are never allowed

  // 此时只会剩下TransactionState::GROWING和TransactionState::SHRINKING两种状态
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ: {
      // Locks should be granted only in growing state
      if (txn->GetState() != TransactionState::GROWING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      // All locks allowed in read_committed, only Is, S locks allowed in shrinking
      if (txn->GetState() != TransactionState::GROWING) {
        if (mode != LockMode::SHARED && mode != LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    }
    case IsolationLevel::READ_UNCOMMITTED: {
      if (mode != LockMode::INTENTION_EXCLUSIVE && mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() != TransactionState::GROWING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
  }
}

auto LockManager::IsTxnHasOidLock(Transaction *txn, LockMode &mode, table_oid_t oid) -> bool {
  if (txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end()) {
    mode = LockMode::INTENTION_SHARED;
    return true;
  }
  if (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) {
    mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
      txn->GetSharedIntentionExclusiveTableLockSet()->end()) {
    mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end()) {
    mode = LockMode::SHARED;
    return true;
  }
  if (txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) {
    mode = LockMode::EXCLUSIVE;
    return true;
  }
  return false;
}

void LockManager::CheckItCanUpgradeOrNot(Transaction *txn, LockMode has_mode, LockMode upgrade_mode) {
  switch (has_mode) {
    case LockMode::INTENTION_SHARED:
      if (upgrade_mode == LockMode::INTENTION_SHARED) {
        this->table_lock_map_latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (upgrade_mode != LockMode::SHARED_INTENTION_EXCLUSIVE && upgrade_mode != LockMode::EXCLUSIVE) {
        this->table_lock_map_latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (upgrade_mode != LockMode::EXCLUSIVE) {
        this->table_lock_map_latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return;
    default: {
      std::cout << "不合法的升级X->S" << std::endl;
      this->table_lock_map_latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
}

void LockManager::ObtainedAllGrantedLockMode(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                             const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto request_queue = lock_request_queue->request_queue_;
  auto it = request_queue.begin();
  // 跳过第一个
  // if(it != request_queue.end()){
  //   it++;
  // }
  while (it != request_queue.end()) {
    if ((*it)->granted_) {
      granted_lock_mode[(*it)->lock_mode_] = true;
    } else {
      break;
    }
    it++;
  }
}

auto LockManager::IsLockCompatible(LockMode hold_mode, LockMode apply_mode) -> bool {
  switch (hold_mode) {
    case LockMode::INTENTION_SHARED: {
      if (apply_mode == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::SHARED: {
      if (apply_mode == LockMode::INTENTION_EXCLUSIVE || apply_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          apply_mode == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::EXCLUSIVE: {
      return false;
    } break;
    case LockMode::INTENTION_EXCLUSIVE: {
      if (apply_mode == LockMode::SHARED || apply_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          apply_mode == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (apply_mode == LockMode::INTENTION_EXCLUSIVE || apply_mode == LockMode::SHARED ||
          apply_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || apply_mode == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
  }
  return true;
}

auto LockManager::IsRequestLockCompatibleWithExistedLockMode(
    const std::unordered_map<LockMode, bool> &granted_lock_mode, LockMode checkmode) -> bool {
  auto it = granted_lock_mode.begin();
  while (it != granted_lock_mode.end()) {
    if (it->second) {
      if (!this->IsLockCompatible(it->first, checkmode)) {
        return false;
      }
    }
    it++;
  }
  return true;
}

#ifdef DEBUG
void LockManager::DebugIsTxnLockRequestGrantedTrue(Transaction *txn,
                                                   const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end()) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      if (!(*it)->granted_) {
        BUSTUB_ASSERT(true, "理应将该锁授予事务,却没有");
      }
    }
  }
}
#endif

void LockManager::DeleteTxnHoldLockFromQueue(Transaction *txn, std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end()) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      lock_request_queue->request_queue_.erase(it);
      break;
    }
    it++;
  }
}

void LockManager::InsertNewTxnRequestLockIntoQueue(Transaction *txn, LockMode insert_lock_mode, table_oid_t table_id,
                                                   std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                                   std::shared_ptr<LockRequest> &new_request, bool insert_front) {
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), insert_lock_mode, table_id);
  new_request = request;
  if (insert_front) {
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    // // 这个地方也许可以判断一下能否grant再插入(针对upgrading的request)
    // std::unordered_map<LockMode,bool> granted_lock_mode;
    // this->ObtainedAllGrantedLockMode(granted_lock_mode,lock_request_queue);
    // auto res = this->IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode,insert_lock_mode);
    // if(res){
    //   request->granted_ = true;
    //   // 这个或许可以最后再插入
    //   this->InsertGrantedLockIntoTxnUnorderedSet(txn,table_id,insert_lock_mode);
    // }

    // lock_request_queue->request_queue_.push_front(std::move(request));
    lock_request_queue->request_queue_.emplace_front(request);
  } else {
    // lock_request_queue->request_queue_.push_back(std::move(request));
    lock_request_queue->request_queue_.emplace_back(request);
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode origin_mode, LockMode upgraded_mode,
                                   std::shared_ptr<LockRequest> &new_request, const table_oid_t &oid) -> int {
  // 1.首先检查能否从origin_mode的锁类型升级到upgraded_mode的锁类型
  CheckItCanUpgradeOrNot(txn, origin_mode, upgraded_mode);

#ifdef DEBUG
  if (table_lock_map_.find(oid) != table_lock_map_.end()) {
    // 说明出现了问题,明明调用前txn已经有了本资源的锁,但是却找不到本资源的锁,是不正常的行为
    BUSTUB_ASSERT(1, "没有拿到本资源的锁");
  }
#endif

  // this->table_lock_map_latch_.lock();
  // 2.获取id为oid的table的锁等待队列
  auto queue_ptr = table_lock_map_[oid];
  std::unique_lock<std::mutex> unique_lock{queue_ptr->latch_};

  // table_lock_map_latch_.unlock();
  if (queue_ptr->upgrading_ != INVALID_TXN_ID) {
    unique_lock.unlock();
    this->table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  queue_ptr->upgrading_ = txn->GetTransactionId();
// 3.与已经授予的该资源的锁进行检查,查看锁升级后会不会与已granted的锁互斥
// std::unordered_map<LockMode,bool> granted_lock_mode;
// this->ObtainedAllGrantedLockMode(granted_lock_mode,queue_ptr);
// (4.1这里可以先尝试queue中txn事务的LockRequest请求的granted是否为true,如果不是说明有问题)
#ifdef DEBUG
  this->DebugIsTxnLockRequestGrantedTrue(txn, queue_ptr);
#endif

  this->DeleteTxnHoldLockFromQueue(txn, queue_ptr);
  this->DeleteLockRecordFromTransaction(txn, oid, origin_mode);
  // 3.将升级的请求插入到队列最前面
  this->InsertNewTxnRequestLockIntoQueue(txn, upgraded_mode, oid, queue_ptr, new_request, true);

  // auto is_upgrade_compatiable = this->IsUpgradeCompatibleWithExistedLockMode(granted_lock_mode,upgraded_mode);
  // if(is_upgrade_compatiable){
  //   this->DeleteTxnHoldLockFromQueue(txn,queue_ptr);
  //   this->
  // }else{

  // }

  // 首先检查是否有锁升级,如果没有返回0
  // LockMode has_mode;
  // if(CheckTableUpgrade(txn,has_mode,oid)){
  //   // 说明当前事务有一个has_mode的锁
  //   // 进行锁升级流程,如果锁升级成功返回1,否则返回2

  //   if(has_mode == lock_mode){
  //     // 一样的话不用升级也不用重复加锁
  //     return 1;
  //   }

  //   // 检查一下加锁请求是否有效
  //   CheckItCanUpgradeOrNot(txn, has_mode, lock_mode);
  //   this->table_lock_map_latch_.lock();
  //   // 获取id为oid的table的锁等待队列
  //   auto queue_ptr = table_lock_map_[oid];
  //   table_lock_map_latch_.unlock();
  //   std::unique_lock<std::mutex> unique_lock{queue_ptr->latch_};
  //   if (queue_ptr->upgrading_ != INVALID_TXN_ID) {
  //     if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
  //       txn->SetState(TransactionState::ABORTED);
  //       throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  //     }
  //   }
  // }
  return 0;
}

void LockManager::DeleteLockRecordFromTransaction(Transaction *txn, table_oid_t table_id, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(table_id);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(table_id);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(table_id);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(table_id);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(table_id);
      break;
  }
}

void LockManager::InsertGrantedLockIntoTxnUnorderedSet(Transaction *txn, table_oid_t table_id, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(table_id);
      break;
  }
}

// auto LockManager::BlockedUtilRequestGranted(Transaction *txn, std::unique_lock<std::mutex> &unique_lock,
//                                             std::condition_variable &condition, LockRequest * request,
//                                             std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool {
//   while (true) {
//     // wait检查request->granted_ || txn->GetState() == TransactionState::ABORTED是否满足,不满足就沉睡,满足接着向下走
//     condition.wait(unique_lock,
//                    [&]() {
//                     return request->granted_ || txn->GetState() == TransactionState::ABORTED;
//                     });
//     // if (txn->GetState() != TransactionState::ABORTED && queue_ptr->request_queue_.front() != request) {
//     //   //
//     如果当前txn被通知了继续运行,首先要看一眼其前面的别的请求是否被满足,如果前面的请求没有被满足,就要优先处理前面的请求

//     //   // 讲道理，我觉得这句话不需要,在Lock中调用notify的行为感觉有点问题。
//     //   // condition.notify_all();
//     //   continue;
//     // }
//     if (txn->GetState() == TransactionState::ABORTED || request->granted_) {
//       break;
//     }
//   }
//   return txn->GetState() == TransactionState::ABORTED;
// }

auto LockManager::BlockedUtilRequestGranted(Transaction *txn, std::unique_lock<std::mutex> &unique_lock,
                                            std::condition_variable &condition, std::shared_ptr<LockRequest> &request,
                                            std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool {
  while (true) {
    // wait检查request->granted_ || txn->GetState() == TransactionState::ABORTED是否满足,不满足就沉睡,满足接着向下走
    condition.wait(unique_lock, [&]() {
      // std::cout << "当前处在BlockedUtilRequestGranted中,queue的大小为" << queue_ptr->request_queue_.size() <<
      // std::endl;

      if (!request->granted_) {
        // std::cout<<"虚假唤醒"<<std::endl;
      }
      // std::cout << "能走到这一步" << std::endl;
      return request->granted_ || txn->GetState() == TransactionState::ABORTED;
    });
    // if (txn->GetState() != TransactionState::ABORTED && queue_ptr->request_queue_.front() != request) {
    //   //
    //   如果当前txn被通知了继续运行,首先要看一眼其前面的别的请求是否被满足,如果前面的请求没有被满足,就要优先处理前面的请求

    //   // 讲道理，我觉得这句话不需要,在Lock中调用notify的行为感觉有点问题。
    //   // condition.notify_all();
    //   continue;
    // }
    if (txn->GetState() == TransactionState::ABORTED || request->granted_) {
      break;
    }
  }
  return txn->GetState() == TransactionState::ABORTED;
}

auto LockManager::IsAllRequestGrantedInQueueExceptLast(std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool {
  auto it = queue_ptr->request_queue_.begin();
  decltype(queue_ptr->request_queue_.size()) num = 0;
  while (it != queue_ptr->request_queue_.end()) {
    if (num == queue_ptr->request_queue_.size() - 1) {
      break;
    }
    if (!((*it)->granted_)) {
      return false;
    }
    num++;
    it++;
  }
  return true;
}

void LockManager::ObtainedAllGrantedLockModeExceptLast(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                                       const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto request_queue = lock_request_queue->request_queue_;
  auto it = request_queue.begin();
  decltype(lock_request_queue->request_queue_.size()) num = 0;
  while (it != request_queue.end()) {
    if (num == lock_request_queue->request_queue_.size() - 1) {
      break;
    }
    if ((*it)->granted_) {
      granted_lock_mode[(*it)->lock_mode_] = true;
    } else {
      break;
    }
    num++;
    it++;
  }
}

void LockManager::CreateNewQueueForTableLockMap(Transaction *txn, table_oid_t table_id, LockMode lock_mode) {
  auto queue_ptr = std::make_shared<LockRequestQueue>();
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, table_id);
  // 这时候没有别的事务和本事务抢夺该table的资源,故可以直接将table资源给予它
  request->granted_ = true;
  // 同时将该锁插入进该事务的对应锁的unordered_set

  this->InsertGrantedLockIntoTxnUnorderedSet(txn, table_id, lock_mode);
  queue_ptr->request_queue_.emplace_back(request);
  table_lock_map_[table_id] = queue_ptr;
}

void LockManager::GrantNewLocksIfPossible(std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  std::unordered_map<LockMode, bool> granted_lock_mode;
  this->ObtainedAllGrantedLockMode(granted_lock_mode, lock_request_queue);
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end()) {
    if ((*it)->granted_) {
      it++;
      continue;
    }
    if (IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode, (*it)->lock_mode_)) {
      // 说明可以授予锁
      (*it)->granted_ = true;
      granted_lock_mode[(*it)->lock_mode_] = true;
    } else {
      // 按照FIFO的顺序,后面的请求都不能获得锁
      break;
    }
    it++;
  }
  lock_request_queue->cv_.notify_all();
}

// void LockManager::GetOldRequestFromQueue(Transaction *txn,std::shared_ptr<LockRequest>&
// old_lock_request,std::shared_ptr<LockRequestQueue> &lock_request_queue){
//   for(auto & it : lock_request_queue->request_queue_){
//     if(it->txn_id_ == txn->GetTransactionId()){
//       old_lock_request = it;
//       break;
//     }
//   }
// }
// void LockManager::GetOldRequestFromQueue(std::shared_ptr<LockRequest>& old_lock_request,
// std::shared_ptr<LockRequestQueue> &lock_request_queue){

// }

void LockManager::ObtainedAllGrantedLockModeExceptFirst(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                                        const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  auto request_queue = lock_request_queue->request_queue_;
  auto it = request_queue.begin();
  // 跳过第一个
  if (it != request_queue.end()) {
    it++;
  }
  while (it != request_queue.end()) {
    if ((*it)->granted_) {
      granted_lock_mode[(*it)->lock_mode_] = true;
    } else {
      break;
    }
    it++;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::cout << "txn号为:" << txn->GetTransactionId() << std::endl;
  std::cout << "正尝试拿到TABLE" << oid << "的";
  switch (lock_mode) {
    case LockMode::SHARED:
      std::cout << "SHARED";
      break;
    case LockMode::EXCLUSIVE:
      std::cout << "EXCLUSIVE";
      break;
    case LockMode::INTENTION_SHARED:
      std::cout << "INTENTION_SHARED";
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      std::cout << "INTENTION_EXCLUSIVE";
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      std::cout << "SHARED_INTENTION_EXCLUSIVE";
      break;
  }
  std::cout << "锁" << std::endl;
  if (txn == nullptr) {
    return false;
  }
  // 1.首先检查事务的状态(若是aborted或者commited直接抛异常),之后再根据事务的隔离级别判断加锁是否正确
  this->TransactionIsolationLevelCheck(txn, lock_mode);
  this->table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) != table_lock_map_.end()) {
    // 2.尝试进行锁升级
    LockMode origin_mode;
    std::shared_ptr<LockRequest> old_lock_request;
    auto is_txn_has_lock = this->IsTxnHasOidLock(txn, origin_mode, oid);
    std::shared_ptr<LockRequest> new_lock_request;
    if (is_txn_has_lock) {
      // 这个地方首先锁定旧的请求的位置

      // 2.1说明这时候txn有一个origin_mode的锁
      // 首先判断origin_mode和lock_mode是不是相同的
      if (origin_mode == lock_mode) {
        // 说明不需要升级,也不需要再做可能的授予锁操作,直接返回true
        this->table_lock_map_latch_.unlock();
        return true;
      }
      // 尝试进行upgrade流程
      this->UpgradeLockTable(txn, origin_mode, lock_mode, new_lock_request, oid);
    } else {
      // 2.1说明这时候只要简单插入到队列尾部就可以了
      auto queue_ptr = table_lock_map_[oid];
      // 这个地方应该不用上锁,因为map的锁还被拿着，上解锁影响效率
      std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
      this->InsertNewTxnRequestLockIntoQueue(txn, lock_mode, oid, queue_ptr, new_lock_request, false);
    }
    // 说明这时候有tid为oid的table有锁请求
    auto queue_ptr = table_lock_map_[oid];
    std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
    this->table_lock_map_latch_.unlock();
    std::unordered_map<LockMode, bool> granted_lock_mode_except_first;
    // 实际上这个地方应该拿除了第一个(锁升级的)以外的锁类型
    this->ObtainedAllGrantedLockModeExceptFirst(granted_lock_mode_except_first, queue_ptr);
    if (is_txn_has_lock) {
      // 如果能走到这一步，说明一定有正确的锁升级
      // 这时候需要首先判断一下自身能不能grant
      if (IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode_except_first, lock_mode)) {
        // 自身能够直接grant
        // 删除txn原先的lock
        queue_ptr->upgrading_ = INVALID_TXN_ID;
        this->DeleteLockRecordFromTransaction(txn, oid, origin_mode);
        // 自身能够grant,首先将granted置为true
        // queue_ptr->request_queue_.front()->granted_ = true;
        new_lock_request->granted_ = true;
        // 更新txn的set集合
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, lock_mode);
      } else {
        // 自身不能够直接grant,则需要直接阻塞住,后来的没有得到锁的请求要等到本request得到锁之后才能去判断能否得到锁(FIFO)
        // auto upgrade_request = queue_ptr->request_queue_.front();
        auto is_aborted = this->BlockedUtilRequestGranted(txn, unique_queue_ptr_latch, queue_ptr->cv_,
                                                          queue_ptr->request_queue_.front(), queue_ptr);
        if (is_aborted) {
          // 如果因为不明原因txn线程苏醒后,发现其状态变为了aborted,就需要将其从请求队列中删除,
          queue_ptr->request_queue_.remove(new_lock_request);
          queue_ptr->upgrading_ = INVALID_TXN_ID;
          GrantNewLocksIfPossible(queue_ptr);
          return false;
        }
        // 这种情况说明其能够进行正常grant,且已修改对应的granted为true(在unlock中做)
        queue_ptr->upgrading_ = INVALID_TXN_ID;
        // this->DeleteLockRecordFromTransaction(txn, oid, origin_mode);
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, lock_mode);
      }
    } else {
      // 说明没有锁升级，这时候要看一下队尾的元素能不能被直接授予,也就是判断一下它之前的request是不是都granted了，并且与其相容
      std::unordered_map<LockMode, bool> granted_lock_mode_except_last;
      this->ObtainedAllGrantedLockModeExceptLast(granted_lock_mode_except_last, queue_ptr);
      // if(!IsAllRequestGrantedInQueueExceptLast(queue_ptr)){
      //   std::cout<<"txn:"<<txn->GetTransactionId()<<"没通过IsAllRequestGrantedInQueueExceptLast"<<std::endl;
      // }
      // if(!IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode_except_last,lock_mode)){
      //   std::cout<<"txn:"<<txn->GetTransactionId()<<"没通过IsRequestLockCompatibleWithExistedLockMode"<<std::endl;
      //   std::cout<<"此时queue的大小为:"<<queue_ptr->request_queue_.size()<<std::endl;
      // }
      if (IsAllRequestGrantedInQueueExceptLast(queue_ptr) &&
          IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode_except_last, lock_mode)) {
        // 说明可以直接授予,不用阻塞
        queue_ptr->request_queue_.back()->granted_ = true;
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, lock_mode);
      } else {
        // 说明需要阻塞,直到某个线程notify唤醒它
        // std::cout<<"不能直接授予,需要前面某个事务放锁"<<std::endl;
        auto is_aborted = BlockedUtilRequestGranted(txn, unique_queue_ptr_latch, queue_ptr->cv_,
                                                    queue_ptr->request_queue_.back(), queue_ptr);
        if (is_aborted) {
          // std::cout<<"因为不明原因被aborted了"<<std::endl;
          // auto normal_requests = queue_ptr->request_queue_.back();
          // 如果因为不明原因txn线程苏醒后,发现其状态变为了aborted,就需要将其从请求队列中删除,
          queue_ptr->request_queue_.remove(new_lock_request);
          // queue_ptr->upgrading_ = INVALID_TXN_ID;
          GrantNewLocksIfPossible(queue_ptr);
          return false;
        }
        // 这种情况说明已经给予锁了，插入新的锁
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, lock_mode);
      }
    }
    // 3. 遍历队列，授予所有能够授予的锁,首先需要给
    GrantNewLocksIfPossible(queue_ptr);
  } else {
    // 说明这时候没有编号为oid的table有锁请求
    // std::cout<<"新建一个请求队列和一个请求"<<std::endl;
    this->CreateNewQueueForTableLockMap(txn, oid, lock_mode);
    this->table_lock_map_latch_.unlock();
  }
  return true;
}

void LockManager::CheckIfTxnHoldRowLock(Transaction *txn, table_oid_t oid) {
  bool have_row_shared_lock = false;
  if (txn->GetSharedRowLockSet()->count(oid) != 0) {
    if (!((*txn->GetSharedRowLockSet())[oid].empty())) {
      have_row_shared_lock = true;
    }
  }
  bool have_row_exclusive_lock = false;
  if (txn->GetExclusiveRowLockSet()->count(oid) != 0) {
    if (!((*txn->GetExclusiveRowLockSet())[oid].empty())) {
      have_row_exclusive_lock = true;
    }
  }
  if (have_row_shared_lock || have_row_exclusive_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
}

auto LockManager::CheckIfTxnHoldTableLock(Transaction *txn, table_oid_t oid, LockMode &hold_lock) -> bool {
  // std::unordered_map<LockMode, bool> is_txn_have_lock;
  std::optional<LockMode> txn_have_mode = std::nullopt;
  if ((txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end())) {
    txn_have_mode.emplace(LockMode::SHARED);
    // is_txn_have_lock[LockMode::SHARED] = true;
  }
  if (txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) {
    txn_have_mode.emplace(LockMode::EXCLUSIVE);
    // is_txn_have_lock[LockMode::EXCLUSIVE] = true;
  }
  if (txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end()) {
    txn_have_mode.emplace(LockMode::INTENTION_SHARED);
    // is_txn_have_lock[LockMode::INTENTION_SHARED] = true;
  }
  if (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) {
    txn_have_mode.emplace(LockMode::INTENTION_EXCLUSIVE);
    // is_txn_have_lock[LockMode::INTENTION_EXCLUSIVE] = true;
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
      txn->GetSharedIntentionExclusiveTableLockSet()->end()) {
    txn_have_mode.emplace(LockMode::SHARED_INTENTION_EXCLUSIVE);
    // is_txn_have_lock[LockMode::SHARED_INTENTION_EXCLUSIVE] = true;
  }
  // if(is_txn_have_lock.size() > 1){
  //   // 未知错误,不知道为什么拿了同一个table的两把锁
  //   std::cout<<"未知错误"<<std::endl;
  // }
  if (txn_have_mode == std::nullopt) {
    // 它没有拿任何锁,抛异常
    // std::cout<<"table:"<<oid<<std::endl;
    // std::cout<<"txn:"<<txn->GetThreadId()<<std::endl;
    // std::cout<<"wtf"<<std::endl;
    // if(hold_lock == LockMode::EXCLUSIVE){

    // }
    return false;
  }

  // for(auto & it : queue_ptr->request_queue_){
  //   if(it->txn_id_ == txn->GetTransactionId()){
  //     if(it->lock_mode_ != *txn_have_mode){
  //       // 对应table的请求队列中，该事务所授予的请求锁和其请求的锁不一致，玄学问题。
  //       std::cout<<"未知错误"<<std::endl;
  //     }
  //   }
  // }
  hold_lock = *txn_have_mode;
  return true;
}

void LockManager::DeleteTxnLockFromTableQueue(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue_ptr) {
  for (auto it = queue_ptr->request_queue_.begin(); it != queue_ptr->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      // 删除这个request
      // std::cout<<"确实删除了"<<std::endl;
      // std::cout<<"删除前:"<<queue_ptr->request_queue_.size()<<std::endl;
      queue_ptr->request_queue_.erase(it);
      // std::cout<<"删除后:"<<queue_ptr->request_queue_.size()<<std::endl;
      break;
    }
  }
}

void LockManager::UpdateTxnStateWhenUnlock(Transaction *txn, const LockMode &hold_lock) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (hold_lock == LockMode::SHARED || hold_lock == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      if (hold_lock == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (hold_lock == LockMode::SHARED) {
        // BUSTUB说这里是未定义的行为,但是没有说抛出什么异常....
        // txn->SetState(TransactionState::ABORTED);
      }
      break;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "txn号为:" << txn->GetTransactionId() << std::endl;
  std::cout << "正尝试给table" << oid;
  std::cout << "解锁" << std::endl;

  // 1.首先检查在释放时是不是还拿着某个行的锁
  this->CheckIfTxnHoldRowLock(txn, oid);
  std::cout << "走到这一步" << std::endl;
  LockMode hold_mode;
  if (!this->CheckIfTxnHoldTableLock(txn, oid, hold_mode)) {
    // 2.如果没有持有请求解锁的锁,就需要抛出ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD异常
    // 记得解锁
    // unique_queue_ptr_latch.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::cout << "怎么回事2?" << std::endl;
  this->table_lock_map_latch_.lock();
  std::cout << "怎么回事3?" << std::endl;
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    // std::cout<<"怎么回事4?"<<std::endl;
    table_lock_map_latch_.unlock();
    // std::cout<<"走到这一步123"<<std::endl;
    return false;
  }
  std::cout << "怎么回事" << std::endl;
  auto queue_ptr = table_lock_map_[oid];
  std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
  std::cout << "解锁走到了这一步" << std::endl;
  this->table_lock_map_latch_.unlock();
  std::cout << "解锁走到了这一步2" << std::endl;
  // 3.能走到这一步,说明需要从请求队列中清除出该请求
  this->DeleteTxnLockFromTableQueue(txn, queue_ptr);
  // 4.根据事务的隔离级别更新事务的状态,不符合要求则抛出异常
  this->UpdateTxnStateWhenUnlock(txn, hold_mode);

  // 5.更新txn的set,删除set对应的table的锁
  this->DeleteLockRecordFromTransaction(txn, oid, hold_mode);
  // 6.给予所有可以给予的锁
  // std::cout<<"解锁走到了这一步"<<std::endl;
  this->GrantNewLocksIfPossible(queue_ptr);
  // 7.通知这个queue上所有沉睡的线程
  queue_ptr->cv_.notify_all();

  return true;
}

void LockManager::CheckIntentionLockOnRow(Transaction *txn, LockMode mode) {
  if (mode != LockMode::SHARED && mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
}

void LockManager::CheckIfWhenLockRowTheTableLockIsRight(Transaction *txn, LockMode mode, table_oid_t oid) {
  if (mode == LockMode::SHARED) {
    auto shared_lock_hold =
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) != 0 ||
        txn->GetSharedTableLockSet()->count(oid) != 0 || txn->GetIntentionSharedTableLockSet()->count(oid) != 0 ||
        txn->GetIntentionExclusiveTableLockSet()->count(oid) != 0 || txn->GetExclusiveTableLockSet()->count(oid) != 0;
    if (shared_lock_hold) {
      return;
    }
  } else if (mode == LockMode::EXCLUSIVE) {
    auto exclusive_lock_hold = txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) != 0 ||
                               txn->GetIntentionExclusiveTableLockSet()->count(oid) != 0 ||
                               txn->GetExclusiveTableLockSet()->count(oid) != 0;
    if (exclusive_lock_hold) {
      return;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
}

auto LockManager::IsTxnHasRidLock(Transaction *txn, LockMode &mode, table_oid_t oid, RID rid) -> bool {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  bool is_have = false;
  if (s_row_lock_set->count(oid) > 0) {
    auto s_rid_row_set = s_row_lock_set->at(oid);
    if (s_rid_row_set.count(rid) > 0) {
      mode = LockMode::SHARED;
      is_have = true;
    }
  }
  if (x_row_lock_set->count(oid) > 0) {
    auto x_rid_row_set = x_row_lock_set->at(oid);
    if (x_rid_row_set.count(rid) > 0) {
      mode = LockMode::EXCLUSIVE;
      is_have = true;
    }
  }
  return is_have;
}

// oid貌似没用，直接用的bustub默认给的函数...感觉可以删掉
auto LockManager::UpgradeLockRow(Transaction *txn, LockMode origin_mode, LockMode upgraded_mode, const table_oid_t &oid,
                                 const RID &rid, std::shared_ptr<LockRequest> &new_request) -> int {
  // 1.首先检查能否从origin_mode的锁类型升级到upgraded_mode的锁类型
  CheckItCanUpgradeOrNot(txn, origin_mode, upgraded_mode);

  // #ifdef DEBUG
  // if(table_lock_map_.find(oid) != table_lock_map_.end()){
  //   // 说明出现了问题,明明调用前txn已经有了本资源的锁,但是却找不到本资源的锁,是不正常的行为
  //   BUSTUB_ASSERT(1,"没有拿到本资源的锁");
  // }
  // #endif

  // this->table_lock_map_latch_.lock();
  // 2.获取id为oid的table的锁等待队列
  auto queue_ptr = row_lock_map_[rid];

  std::unique_lock<std::mutex> unique_lock{queue_ptr->latch_};

  // table_lock_map_latch_.unlock();
  if (queue_ptr->upgrading_ != INVALID_TXN_ID) {
    unique_lock.unlock();
    this->row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  queue_ptr->upgrading_ = txn->GetTransactionId();
// 3.与已经授予的该资源的锁进行检查,查看锁升级后会不会与已granted的锁互斥
// std::unordered_map<LockMode,bool> granted_lock_mode;
// this->ObtainedAllGrantedLockMode(granted_lock_mode,queue_ptr);
// (4.1这里可以先尝试queue中txn事务的LockRequest请求的granted是否为true,如果不是说明有问题)
#ifdef DEBUG
  this->DebugIsTxnLockRequestGrantedTrue(txn, queue_ptr);
#endif

  this->DeleteTxnHoldLockFromQueue(txn, queue_ptr);
  this->DeleteLockRecordFromTransaction(txn, oid, rid, origin_mode);
  // 3.将已经升级的请求插入到队列最前面
  this->InsertNewTxnRequestLockIntoQueue(txn, upgraded_mode, oid, rid, queue_ptr, new_request, true);
  return 0;
}

void LockManager::InsertNewTxnRequestLockIntoQueue(Transaction *txn, LockMode insert_lock_mode, const table_oid_t &oid,
                                                   const RID &rid,
                                                   std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                                   std::shared_ptr<LockRequest> &new_request, bool insert_front) {
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), insert_lock_mode, oid, rid);
  new_request = request;
  if (insert_front) {
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    // // 这个地方也许可以判断一下能否grant再插入(针对upgrading的request)
    // std::unordered_map<LockMode,bool> granted_lock_mode;
    // this->ObtainedAllGrantedLockMode(granted_lock_mode,lock_request_queue);
    // auto res = this->IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode,insert_lock_mode);
    // if(res){
    //   request->granted_ = true;
    //   // 这个或许可以最后再插入
    //   this->InsertGrantedLockIntoTxnUnorderedSet(txn,table_id,insert_lock_mode);
    // }
    lock_request_queue->request_queue_.emplace_front(request);
  } else {
    lock_request_queue->request_queue_.emplace_back(request);
  }
}

void LockManager::DeleteLockRecordFromTransaction(Transaction *txn, const table_oid_t &oid, const RID &rid,
                                                  LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      // 不可能的情况
    }
  }
}

void LockManager::InsertGrantedLockIntoTxnUnorderedSet(Transaction *txn, const table_oid_t &oid, const RID &rid,
                                                       LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED: {
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      // txn->GetSharedRowLockSet()->at(oid).insert(rid);
      // std::cout<<"运行到这一步3"<<std::endl;
    } break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      // txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      // 不可能的情况
    }
  }
}

void LockManager::CreateNewQueueForRowLockMap(Transaction *txn, const table_oid_t &oid, const RID &rid,
                                              LockMode lock_mode) {
  // std::cout<<"运行到这一步1"<<std::endl;
  auto queue_ptr = std::make_shared<LockRequestQueue>();

  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  // 这时候没有别的事务和本事务抢夺该table的资源,故可以直接将table资源给予它
  request->granted_ = true;
  // 同时将该锁插入进该事务的对应锁的unordered_set
  // std::cout<<"运行到这一步2"<<std::endl;
  this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, rid, lock_mode);

  queue_ptr->request_queue_.push_back(request);

  row_lock_map_[rid] = queue_ptr;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << "txn号为:" << txn->GetTransactionId() << std::endl;
  std::cout << "正尝试拿到ROW" << rid.GetSlotNum() << "的";
  switch (lock_mode) {
    case LockMode::SHARED:
      std::cout << "SHARED";
      break;
    case LockMode::EXCLUSIVE:
      std::cout << "EXCLUSIVE";
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  std::cout << "锁" << std::endl;

  if (txn == nullptr) {
    return false;
  }
  // 1.首先检查事务的状态(若是aborted或者commited直接抛异常),之后再根据事务的隔离级别判断加锁是否正确
  this->TransactionIsolationLevelCheck(txn, lock_mode);
  // 2.判断是否错误地上了意向锁
  this->CheckIntentionLockOnRow(txn, lock_mode);
  // 3.row需要判断对应的Table有没有对应的Lock
  this->CheckIfWhenLockRowTheTableLockIsRight(txn, lock_mode, oid);

  this->row_lock_map_latch_.lock();

  if (row_lock_map_.find(rid) != row_lock_map_.end()) {
    // 2.尝试进行锁升级
    LockMode origin_mode;
    auto is_txn_has_lock = this->IsTxnHasRidLock(txn, origin_mode, oid, rid);
    std::shared_ptr<LockRequest> new_lock_request;
    if (is_txn_has_lock) {
      // 2.1说明这时候txn有一个origin_mode的锁
      // 首先判断origin_mode和lock_mode是不是相同的
      if (origin_mode == lock_mode) {
        this->row_lock_map_latch_.unlock();
        // 说明不需要升级,也不需要再做可能的授予锁操作,直接返回true
        return true;
      }
      // 尝试进行upgrade流程
      this->UpgradeLockRow(txn, origin_mode, lock_mode, oid, rid, new_lock_request);
    } else {
      // 2.1说明这时候只要简单插入到队列尾部就可以了
      auto queue_ptr = row_lock_map_[rid];
      std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
      this->InsertNewTxnRequestLockIntoQueue(txn, lock_mode, oid, rid, queue_ptr, new_lock_request, false);
    }
    // 说明这时候有tid为oid的table有锁请求
    auto queue_ptr = row_lock_map_[rid];
    std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
    this->row_lock_map_latch_.unlock();

    std::unordered_map<LockMode, bool> granted_lock_mode_except_first;
    this->ObtainedAllGrantedLockModeExceptFirst(granted_lock_mode_except_first, queue_ptr);
    if (is_txn_has_lock) {
      // 如果能走到这一步，说明一定有正确的锁升级
      // 这时候需要首先判断一下自身能不能grant
      if (IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode_except_first, lock_mode)) {
        // 自身能够直接grant
        // 删除txn原先的lock
        queue_ptr->upgrading_ = INVALID_TXN_ID;
        this->DeleteLockRecordFromTransaction(txn, oid, rid, origin_mode);
        // 自身能够grant,首先将granted置为true
        // queue_ptr->request_queue_.front()->granted_ = true;
        new_lock_request->granted_ = true;

        // 更新txn的set集合
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, rid, lock_mode);
      } else {
        // 自身不能够直接grant,则需要直接阻塞住,后来的没有得到锁的请求要等到本request得到锁之后才能去判断能否得到锁(FIFO)
        // auto upgrade_request = queue_ptr->request_queue_.front();
        auto is_aborted = this->BlockedUtilRequestGranted(txn, unique_queue_ptr_latch, queue_ptr->cv_,
                                                          queue_ptr->request_queue_.front(), queue_ptr);
        if (is_aborted) {
          // 如果因为不明原因txn线程苏醒后,发现其状态变为了aborted,就需要将其从请求队列中删除,
          queue_ptr->request_queue_.remove(new_lock_request);
          queue_ptr->upgrading_ = INVALID_TXN_ID;
          GrantNewLocksIfPossible(queue_ptr);
          // queue_ptr->cv_.notify_all();
          return false;
        }
        // 这种情况说明其能够进行正常grant,且已修改对应的granted为true(在unlock中做)
        queue_ptr->upgrading_ = INVALID_TXN_ID;

        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, rid, lock_mode);
      }
    } else {
      // 说明没有锁升级，这时候要看一下队尾的元素能不能被直接授予,也就是判断一下它之前的request是不是都granted了，并且与其相容
      std::unordered_map<LockMode, bool> granted_lock_mode_except_last;
      this->ObtainedAllGrantedLockModeExceptLast(granted_lock_mode_except_last, queue_ptr);
      if (IsAllRequestGrantedInQueueExceptLast(queue_ptr) &&
          IsRequestLockCompatibleWithExistedLockMode(granted_lock_mode_except_last, lock_mode)) {
        // 说明可以直接授予,不用阻塞
        queue_ptr->request_queue_.back()->granted_ = true;
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, rid, lock_mode);
      } else {
        // 说明需要阻塞,直到某个线程notify唤醒它
        // std::cout << "txn:" << txn->GetTransactionId() << "被阻塞了" << std::endl;
        auto is_aborted = BlockedUtilRequestGranted(txn, unique_queue_ptr_latch, queue_ptr->cv_,
                                                    queue_ptr->request_queue_.back(), queue_ptr);
        if (is_aborted) {
          // std::cout << "txn:" << txn->GetTransactionId() << "被aborted了" << std::endl;

          // auto normal_requests = queue_ptr->request_queue_.back();
          // 如果因为不明原因txn线程苏醒后,发现其状态变为了aborted,就需要将其从请求队列中删除,
          queue_ptr->request_queue_.remove(new_lock_request);
          // queue_ptr->upgrading_ = INVALID_TXN_ID;
          GrantNewLocksIfPossible(queue_ptr);
          // queue_ptr->cv_.notify_all();
          return false;
        }
        // std::cout << "txn:" << txn->GetTransactionId() << "从阻塞中正常恢复" << std::endl;
        // 这种情况说明已经给予锁了，插入新的锁
        this->InsertGrantedLockIntoTxnUnorderedSet(txn, oid, rid, lock_mode);
      }
    }
    // 3. 遍历队列，授予所有能够授予的锁,首先需要给
    GrantNewLocksIfPossible(queue_ptr);
  } else {
    // 说明这时候没有编号为oid的row有锁请求

    this->CreateNewQueueForRowLockMap(txn, oid, rid, lock_mode);
    this->row_lock_map_latch_.unlock();
  }
  return true;
}

auto LockManager::CheckIfTxnHoldRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode &hold_lock)
    -> bool {
  std::optional<LockMode> txn_have_mode = std::nullopt;
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end())) {
    if (txn->GetSharedRowLockSet()->at(oid).count(rid) > 0) {
      txn_have_mode.emplace(LockMode::SHARED);
    }
  }
  if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end()) {
    if (txn->GetExclusiveRowLockSet()->at(oid).count(rid) > 0) {
      txn_have_mode.emplace(LockMode::EXCLUSIVE);
    }
  }
  if (txn_have_mode == std::nullopt) {
    // 它没有拿任何锁,抛异常
    return false;
  }
  hold_lock = *txn_have_mode;
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  std::cout << "txn号为:" << txn->GetTransactionId() << std::endl;
  std::cout << "正尝试给row" << rid.GetSlotNum();
  std::cout << "解锁" << std::endl;
  LockMode hold_mode;
  if (!this->CheckIfTxnHoldRowLock(txn, oid, rid, hold_mode)) {
    // 2.如果没有持有请求解锁的锁,就需要抛出ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD异常
    // 记得解锁
    std::cout << "卧槽,怎么没有拿row的锁" << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  this->row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    return false;
  }
  auto queue_ptr = row_lock_map_[rid];
  std::unique_lock<std::mutex> unique_queue_ptr_latch{queue_ptr->latch_};
  this->row_lock_map_latch_.unlock();

  // 3.能走到这一步,说明需要从请求队列中清除出该请求
  this->DeleteTxnLockFromTableQueue(txn, queue_ptr);

  // this->UpdateTxnStateWhenUnlock(txn, hold_mode);
  // 4.更新txn的set,删除set对应的table的锁
  this->DeleteLockRecordFromTransaction(txn, oid, rid, hold_mode);
  // 5.给予所有可以给予的锁
  this->GrantNewLocksIfPossible(queue_ptr);

  // 6.根据事务的隔离级别更新事务的状态,不符合要求则抛出异常
  if (!force) {
    // 如果没有force，要强制改变事务的状态
    UpdateTxnStateWhenUnlock(txn, hold_mode);
  }
  // 7.通知这个queue上所有沉睡的线程
  queue_ptr->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    std::set<txn_id_t> temp;
    waits_for_[t1] = temp;
  }
  waits_for_[t1].insert(t2);
  // waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // waits_for_latch_.lock();
  waits_for_[t1].erase(t2);
  // waits_for_latch_.unlock();
}

void LockManager::BuildGraph() {
  waits_for_latch_.lock();
  waits_for_.clear();
  waits_for_latch_.unlock();
  // txn_vertex_.clear();
  std::set<txn_id_t> vertexs;
  this->table_lock_map_latch_.lock();
  for (auto &v : table_lock_map_) {
    // auto oid =v.first;
    auto queue_ptr = v.second;
    for (auto &queue_it : queue_ptr->request_queue_) {
      vertexs.insert(queue_it->txn_id_);
      if (queue_it->granted_) {
        for (auto &not_granted_it : queue_ptr->request_queue_) {
          if (!not_granted_it->granted_) {
            AddEdge(not_granted_it->txn_id_, queue_it->txn_id_);
          }
        }
      }
    }
  }
  this->table_lock_map_latch_.unlock();
  this->row_lock_map_latch_.lock();
  for (auto &v : row_lock_map_) {
    // auto oid =v.first;
    auto queue_ptr = v.second;
    for (auto &queue_it : queue_ptr->request_queue_) {
      vertexs.insert(queue_it->txn_id_);
      if (queue_it->granted_) {
        for (auto &not_granted_it : queue_ptr->request_queue_) {
          if (!not_granted_it->granted_) {
            AddEdge(not_granted_it->txn_id_, queue_it->txn_id_);
          }
        }
      }
    }
  }
  this->row_lock_map_latch_.unlock();
  // for(auto &it:vertexs){
  //   this->txn_vertex_.push_back(it);
  // }
}

auto LockManager::Dfs(txn_id_t now, txn_id_t *lowest_txn_id) -> bool {
  if (*lowest_txn_id != -233) {
    return true;
  }
  if (is_in_stack_[now]) {
    bool loop_start = false;
    for (auto &it : now_stack_) {
      if (it == now) {
        loop_start = true;
      }
      if (loop_start) {
        if (*lowest_txn_id == -233) {
          *lowest_txn_id = it;
        } else {
          *lowest_txn_id = std::max(*lowest_txn_id, it);
        }
      }
    }
    return true;
  }
  if (now_is_visited_[now]) {
    return false;
  }
  now_is_visited_[now] = true;
  now_stack_.push_back(now);
  is_in_stack_[now] = true;
  bool ret = false;
  for (int it : waits_for_[now]) {
    ret |= Dfs(it, lowest_txn_id);
  }
  now_stack_.pop_back();
  is_in_stack_[now] = false;
  return ret;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (const auto &it : this->waits_for_) {
    txn_id_t lowest_id = -233;
    if (!now_is_visited_[it.first]) {
      if (Dfs(it.first, &lowest_id)) {
        *txn_id = lowest_id;
        this->is_in_stack_.clear();
        this->now_is_visited_.clear();
        return true;
      }
    }
  }
  this->is_in_stack_.clear();
  this->now_is_visited_.clear();
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  auto it = waits_for_.begin();
  for (; it != waits_for_.end(); it++) {
    for (auto x : it->second) {
      std::pair<txn_id_t, txn_id_t> pair{it->first, x};
      edges.push_back(pair);
    }
  }
  return edges;
}

void LockManager::RemoveAbortedTxn(txn_id_t aborted_id) {
  std::cout << "要删除的号码为:" << aborted_id << std::endl;
  table_lock_map_latch_.lock();
  for (const auto &item : table_lock_map_) {
    auto queue_ptr = item.second;
    queue_ptr->latch_.lock();
    for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == aborted_id) {
        GrantNewLocksIfPossible(queue_ptr);
        queue_ptr->cv_.notify_all();
      }
    }
    queue_ptr->latch_.unlock();
  }
  table_lock_map_latch_.unlock();
  row_lock_map_latch_.lock();
  for (const auto &item : row_lock_map_) {
    auto queue_ptr = item.second;
    queue_ptr->latch_.lock();
    for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == aborted_id) {
        GrantNewLocksIfPossible(queue_ptr);
        queue_ptr->cv_.notify_all();
      }
    }
    queue_ptr->latch_.unlock();
  }
  row_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      this->BuildGraph();
      txn_id_t aborted_tid;
      if (HasCycle(&aborted_tid)) {
        txn_manager_->GetTransaction(aborted_tid)->SetState(TransactionState::ABORTED);
        RemoveAbortedTxn(aborted_tid);
      }
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
