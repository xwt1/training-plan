//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() = default;

  void StartDeadlockDetection() {
    BUSTUB_ENSURE(txn_manager_ != nullptr, "txn_manager_ is not set.")
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    UnlockAll();

    enable_cycle_detection_ = false;

    if (cycle_detection_thread_ != nullptr) {
      cycle_detection_thread_->join();
      delete cycle_detection_thread_;
    }
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  TransactionManager *txn_manager_;

 private:
  /** Spring 2023 */
  /* You are allowed to modify all functions below. */

  /**
   * table:
   */
  /** 检查事务的隔离级别*/
  void TransactionIsolationLevelCheck(Transaction *txn, LockMode mode);

  /** 查看此事务之前是否已经有过table的锁*/
  auto IsTxnHasOidLock(Transaction *txn, LockMode &mode, table_oid_t oid) -> bool;

  void CheckItCanUpgradeOrNot(Transaction *txn, LockMode has_mode, LockMode upgrade_mode);

  /** 从lock_queue中获取锁*/
  void GetLockStatesFromQueue(std::unordered_set<LockMode> &lock_modes, std::shared_ptr<LockRequestQueue> &queue_ptr,
                              bool mode, txn_id_t txn_id);

  /** 插入一个空的锁请求队列*/
  void CreateNewQueueForTableLockMap(Transaction *txn, table_oid_t table_id, LockMode lock_mode);

  /** 删除指定的锁从对应txn的锁类型unordered_set中*/
  void DeleteLockRecordFromTransaction(Transaction *txn, table_oid_t table_id, LockMode lock_mode);

  /** 插入一个可以授予的锁进入对应txn的锁类型unordered_set中*/
  void InsertGrantedLockIntoTxnUnorderedSet(Transaction *txn, table_oid_t table_id, LockMode lock_mode);

  /** 尝试升级将锁从origin_mode升级到upgraded_mode*/
  auto UpgradeLockTable(Transaction *txn, LockMode origin_mode, LockMode upgraded_mode,
                        std::shared_ptr<LockRequest> &new_request, const table_oid_t &oid) -> int;

  /** 获取所有已经granted的锁类型*/
  void ObtainedAllGrantedLockMode(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                  const std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 检查已拥有的锁是否与某请求所需要的锁兼容*/
  auto IsLockCompatible(LockMode hold_mode, LockMode apply_mode) -> bool;

  /** 进行相容性检查*/
  auto IsRequestLockCompatibleWithExistedLockMode(const std::unordered_map<LockMode, bool> &granted_lock_mode,
                                                  LockMode checkmode) -> bool;

  /** debug queue中txn事务的LockRequest请求的granted是否为true,如果不是说明有问题*/
  void DebugIsTxnLockRequestGrantedTrue(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 将Txn已拥有的锁释放掉*/
  void DeleteTxnHoldLockFromQueue(Transaction *txn, std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 将新的事务请求的锁插入进请求队列,根据是否升级插入到队列最前面或者最后面*/
  void InsertNewTxnRequestLockIntoQueue(Transaction *txn, LockMode insert_lock_mode, table_oid_t table_id,
                                        std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                        std::shared_ptr<LockRequest> &new_request, bool insert_front);

  /** 将自身阻塞住,等待notify可以授予*/
  // auto BlockedUtilRequestGranted(Transaction *txn, std::unique_lock<std::mutex> &unique_lock,
  //                                           std::condition_variable &condition, LockRequest * request,
  //                                           std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool;

  auto BlockedUtilRequestGranted(Transaction *txn, std::unique_lock<std::mutex> &unique_lock,
                                 std::condition_variable &condition, std::shared_ptr<LockRequest> &request,
                                 std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool;

  /** 查看是不是所有在队列中的请求除了最后一个都已经被授予了*/
  auto IsAllRequestGrantedInQueueExceptLast(std::shared_ptr<LockRequestQueue> &queue_ptr) -> bool;

  /** 获取除了队列最后一个元素外的所有已经granted的锁类型*/
  void ObtainedAllGrantedLockModeExceptLast(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                            const std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 检查在释放表锁时,是不是还有某个行的锁还没有释放*/
  void CheckIfTxnHoldRowLock(Transaction *txn, table_oid_t oid);

  /** 检查在事务是否持有当前Table的锁*/
  auto CheckIfTxnHoldTableLock(Transaction *txn, table_oid_t oid, LockMode &hold_lock) -> bool;

  /** 删除对应Table队列中请求*/
  void DeleteTxnLockFromTableQueue(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue_ptr);

  /** unlock时更新事务的状态*/
  void UpdateTxnStateWhenUnlock(Transaction *txn, const LockMode &hold_lock);

  /** 检查是否加了意向锁在row上*/
  void CheckIntentionLockOnRow(Transaction *txn, LockMode mode);

  /** 检查在给行加锁的时候给对应的表加的锁是否正确*/
  void CheckIfWhenLockRowTheTableLockIsRight(Transaction *txn, LockMode mode, table_oid_t oid);

  /** 查看此事务之前是否已经有过某个table的row的锁*/
  auto IsTxnHasRidLock(Transaction *txn, LockMode &mode, table_oid_t oid, RID rid) -> bool;

  /** 尝试升级行锁*/
  auto UpgradeLockRow(Transaction *txn, LockMode origin_mode, LockMode upgraded_mode, const table_oid_t &oid,
                      const RID &rid, std::shared_ptr<LockRequest> &new_request) -> int;

  /** 重载,尝试产生一个新的行锁请求,并插入到对应队列中*/
  void InsertNewTxnRequestLockIntoQueue(Transaction *txn, LockMode insert_lock_mode, const table_oid_t &oid,
                                        const RID &rid, std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                        std::shared_ptr<LockRequest> &new_request, bool insert_front);

  /** 重载,尝试从事务的row_set中删除对应rid的请求*/
  void DeleteLockRecordFromTransaction(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode lock_mode);

  /** 重载,尝试插入新的请求进入row_set*/
  void InsertGrantedLockIntoTxnUnorderedSet(Transaction *txn, const table_oid_t &oid, const RID &rid,
                                            LockMode lock_mode);

  /** 创建一个新的请求队列*/
  void CreateNewQueueForRowLockMap(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode lock_mode);

  /** 检查事务是否拿着一个行的锁*/
  auto CheckIfTxnHoldRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode &hold_lock) -> bool;

  auto AreLocksCompatible(LockMode l1, LockMode l2) -> bool;
  auto CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool;

  /** 尝试将队列中能够授予的锁都授予出去*/
  void GrantNewLocksIfPossible(std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 跳过第一个的ObtainedAllGrantedLockModeExceptFirst*/
  void ObtainedAllGrantedLockModeExceptFirst(std::unordered_map<LockMode, bool> &granted_lock_mode,
                                             const std::shared_ptr<LockRequestQueue> &lock_request_queue);

  /** 获取请求队列中某一个事务对某一个table或row的旧的请求*/
  // void GetOldRequestFromQueue(Transaction *txn,std::shared_ptr<LockRequest>&
  // old_lock_request,std::shared_ptr<LockRequestQueue> &lock_request_queue); void
  // GetOldRequestFromQueue(std::shared_ptr<LockRequest>& old_lock_request, const table_oid_t& oid,const RID& rid
  // ,std::shared_ptr<LockRequestQueue> &lock_request_queue);

  auto CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool;
  auto CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool;
  auto FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                 std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool;
  void UnlockAll();
  /** 从小到大获取所有当前的结点集合*/
  // void ObtainAllTxnID(std::vector<txn_id_t> &vertex_vec);

  auto Dfs(txn_id_t now, txn_id_t *lowest_txn_id) -> bool;

  /** 构建图*/
  void BuildGraph();

  /** 移除被放弃的进程*/
  void RemoveAbortedTxn(txn_id_t aborted_id);

  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::map<txn_id_t, std::set<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  // 存在的顶点集合
  std::unordered_map<txn_id_t, bool> now_is_visited_;
  std::unordered_map<txn_id_t, bool> is_in_stack_;
  // 用vector模拟栈
  std::vector<txn_id_t> now_stack_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode x, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (x) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        name = "EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        name = "INTENTION_EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::SHARED:
        name = "SHARED";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        name = "INTENTION_SHARED";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        name = "SHARED_INTENTION_EXCLUSIVE";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
