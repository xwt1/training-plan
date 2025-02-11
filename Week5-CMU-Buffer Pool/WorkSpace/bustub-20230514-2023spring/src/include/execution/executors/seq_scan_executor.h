//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "concurrency/lock_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class LockManager;

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  ~SeqScanExecutor() override;

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void AcquireTableLock(const LockManager::LockMode &request_lock_mode, const table_oid_t &oid);
  void ReleaseTableLock(const table_oid_t &oid);

  void AcquireRowLock(const LockManager::LockMode &request_lock_mode, const table_oid_t &oid, const RID &rid);
  void ReleaseRowLock(const table_oid_t &oid, const RID &rid, bool force);

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  /* 存储table的源信息,在init的时候存储下来,方便Next使用 */
  // TableHeap * table_;  // table本身
  TableIterator *it_;
};
}  // namespace bustub
