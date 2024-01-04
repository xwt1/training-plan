/**
 * lock_manager_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "common_checker.h"  // NOLINT
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  bool correct = true;
  correct = correct && (*txn->GetSharedRowLockSet())[oid].size() == shared_size;
  correct = correct && (*txn->GetExclusiveRowLockSet())[oid].size() == exclusive_size;
  if (!correct) {
    fmt::print("row lock size incorrect for txn={} oid={}: expected (S={} X={}), actual (S={} X={})\n",
               txn->GetTransactionId(), oid, shared_size, exclusive_size, (*txn->GetSharedRowLockSet())[oid].size(),
               (*txn->GetExclusiveRowLockSet())[oid].size());
  }
  EXPECT_TRUE(correct);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void COUT(const std::string &s123, std::mutex &m123) {
  m123.lock();
  std::cout << s123 << std::endl;
  m123.unlock();
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  bool correct = true;
  correct = correct && s_size == txn->GetSharedTableLockSet()->size();
  correct = correct && x_size == txn->GetExclusiveTableLockSet()->size();
  correct = correct && is_size == txn->GetIntentionSharedTableLockSet()->size();
  correct = correct && ix_size == txn->GetIntentionExclusiveTableLockSet()->size();
  correct = correct && six_size == txn->GetSharedIntentionExclusiveTableLockSet()->size();
  if (!correct) {
    fmt::print(
        "table lock size incorrect for txn={}: expected (S={} X={}, IS={}, IX={}, SIX={}), actual (S={} X={}, IS={}, "
        "IX={}, "
        "SIX={})\n",
        txn->GetTransactionId(), s_size, x_size, is_size, ix_size, six_size, txn->GetSharedTableLockSet()->size(),
        txn->GetExclusiveTableLockSet()->size(), txn->GetIntentionSharedTableLockSet()->size(),
        txn->GetIntentionExclusiveTableLockSet()->size(), txn->GetSharedIntentionExclusiveTableLockSet()->size());
  }
  EXPECT_TRUE(correct);
}

void TableLockTest1() {
  std::mutex cout_m1;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;
  std::cout << 1 << std::endl;

  /** 10 tables */
  int num_oids = 100;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  std::cout << 2 << std::endl;
  /** Each transaction takes an X lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    cout_m1.lock();
    // std::cout<<"准备给所有table上锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    cout_m1.unlock();
    for (const table_oid_t &oid : oids) {
      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"准备给"<<"oid: "<<oid<<"上锁"<<std::endl;
      // cout_m1.unlock();
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"已给"<<"oid: "<<oid<<"上锁"<<std::endl;
      // cout_m1.unlock();
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    // cout_m1.lock();
    // std::cout<<"准备给所有table解锁了:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();
    for (const table_oid_t &oid : oids) {
      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"准备给"<<"oid: "<<oid<<"解锁"<<std::endl;
      // cout_m1.unlock();
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"已给"<<"oid: "<<oid<<"解锁"<<std::endl;
      // cout_m1.unlock();
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    // cout_m1.lock();
    // std::cout<<"已全部解锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, ENABLED_TableLockTest1) {
  // for(int i=0;i<10;i++){
  TableLockTest1();
  // std::cout<<"结束了"<<std::endl;
  // }
}  // NOLINT

/** Upgrading single transaction from S -> X */
void TableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  /** Take S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}
TEST(LockManagerTest, ENABLED_TableLockUpgradeTest1) { TableLockUpgradeTest1(); }  // NOLINT

void RowLockTest1() {
  // std::mutex cout_m1;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  // int num_txns = 1;
  int num_txns = 3;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);

    // cout_m1.lock();
    // std::cout<<"准备给所有row上锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    // cout_m1.lock();
    // std::cout<<"准备给所有row解锁了:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    // cout_m1.lock();
    // std::cout<<"已全部解锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(false, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}
TEST(LockManagerTest, ENABLED_RowLockTest1) { RowLockTest1(); }  // NOLINT

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest, ENABLED_TwoPLTest1) { TwoPLTest1(); }  // NOLINT

void AbortTest1() {
  fmt::print(stderr, "AbortTest1: multiple X should block\n");

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  auto txn1 = txn_mgr.Begin();
  auto txn2 = txn_mgr.Begin();
  auto txn3 = txn_mgr.Begin();
  std::cout << "txn2id:" << txn2->GetTransactionId() << std::endl;
  /** All takes IX lock on table */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 0, 0, 1, 0);
  EXPECT_EQ(true, lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn2, 0, 0, 0, 1, 0);
  EXPECT_EQ(true, lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn3, 0, 0, 0, 1, 0);
  std::cout << "走到这一步了1" << std::endl;

  /** txn1 takes X lock on row */
  EXPECT_EQ(true, lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, oid, rid));
  CheckTxnRowLockSize(txn1, oid, 0, 1);

  std::cout << "走到这一步了2" << std::endl;
  /** txn2 attempts X lock on table but should be blocked */
  auto txn2_task = std::thread{[&]() { lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, oid, rid); }};

  std::cout << "走到这一步了3" << std::endl;
  /** Sleep for a bit */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  /** txn2 shouldn't have been granted the lock */
  CheckTxnRowLockSize(txn2, oid, 0, 0);
  std::cout << "走到这一步了4" << std::endl;

  /** txn3 attempts X lock on row but should be blocked */
  auto txn3_task = std::thread{[&]() { lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, oid, rid); }};
  std::cout << "走到这一步了5" << std::endl;

  /** Sleep for a bit */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  /** txn3 shouldn't have been granted the lock */
  CheckTxnRowLockSize(txn3, oid, 0, 0);

  /** Abort txn2 */
  txn_mgr.Abort(txn2);
  std::cout << "走到这一步了6" << std::endl;
  /** txn1 releases lock */
  EXPECT_EQ(true, lock_mgr.UnlockRow(txn1, oid, rid));
  CheckTxnRowLockSize(txn1, oid, 0, 0);

  txn2_task.join();

  txn3_task.join();
  std::cout << "走到这一步了end" << std::endl;
  /** txn2 shouldn't have any row locks */
  CheckTxnRowLockSize(txn2, oid, 0, 0);
  CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
  /** txn3 should have the row lock */
  CheckTxnRowLockSize(txn3, oid, 0, 1);

  delete txn1;
  delete txn2;
  delete txn3;
}

TEST(LockManagerTest, ENABLED_RowAbortTest1) { AbortTest1(); }  // NOLINT

void MyTableLockUpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  table_oid_t toid{0};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    auto res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, toid);
    EXPECT_EQ(true, res);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // upgrade
    // blocked (wait for txn1 to release)
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // release
    lock_mgr.UnlockTable(txn0, toid);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, toid);
    EXPECT_EQ(res, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    // release
    lock_mgr.UnlockTable(txn1, toid);
    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // blocked (wait for txn1 to release)
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock_mgr.UnlockTable(txn2, toid);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}
TEST(LockManagerTest, MyTableLockUpgradeTest) { MyTableLockUpgradeTest(); }  // NOLINT

void MyAbortTest() {
  fmt::print(stderr, "AbortTest1: multiple X should block\n");

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    auto res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // abort txn1
    txn_mgr.Abort(txn1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    lock_mgr.UnlockTable(txn0, toid);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // blocked
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(res, false);
    EXPECT_EQ(txn1->GetState(), TransactionState::ABORTED);
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // blocked
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    lock_mgr.UnlockTable(txn2, toid);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}
TEST(LockManagerTest, AbortTest2) { MyAbortTest(); }  // NOLINT

/** IS, IS, X integration test */
void MyBlockTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  table_oid_t toid{0};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    auto res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    lock_mgr.UnlockTable(txn0, toid);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CheckTableLockSizes(txn1, 0, 0, 0, 1, 0);
    CheckTableLockSizes(txn2, 0, 0, 1, 0, 0);
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // blocked
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    lock_mgr.UnlockTable(txn1, toid);
    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // blocked
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, toid);
    EXPECT_EQ(res, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    lock_mgr.UnlockTable(txn2, toid);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}
TEST(LockManagerTest, BlockTest) { MyBlockTest(); }  // NOLINT

void MyTableLockTest1() {
  std::mutex cout_m1;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;
  std::cout << 1 << std::endl;

  /** 10 tables */
  int num_oids = 1;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  std::cout << 2 << std::endl;
  /** Each transaction takes an X lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    cout_m1.lock();
    // std::cout<<"准备给所有table上锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    cout_m1.unlock();
    // for (const table_oid_t &oid : oids) {
    //   // cout_m1.lock();
    //   // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"准备给"<<"oid: "<<oid<<"上锁"<<std::endl;
    //   // cout_m1.unlock();
    //   res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
    //   // cout_m1.lock();
    //   // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"已给"<<"oid: "<<oid<<"上锁"<<std::endl;
    //   // cout_m1.unlock();
    //   EXPECT_TRUE(res);
    //   CheckGrowing(txns[txn_id]);
    // }
    // cout_m1.lock();
    // std::cout<<"准备给所有table解锁了:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();
    for (const table_oid_t &oid : oids) {
      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"准备给"<<"oid: "<<oid<<"解锁"<<std::endl;
      // cout_m1.unlock();
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      std::cout << res << std::endl;

      // cout_m1.lock();
      // std::cout<<"txn_: "<<txns[txn_id]->GetTransactionId()<<"已给"<<"oid: "<<oid<<"解锁"<<std::endl;
      // cout_m1.unlock();
      // EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    // cout_m1.lock();
    // std::cout<<"已全部解锁:"<<"txn_: "<<txns[txn_id]->GetTransactionId()<<std::endl;
    // cout_m1.unlock();
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, ENABLED_MYTableLockTest1) {
  // for(int i=0;i<10;i++){
  MyTableLockTest1();
  // std::cout<<"结束了"<<std::endl;
  // }
}  // NOLINT

// /** Upgrading single transaction from S -> X */
// void MyTableLockUpgradeTest1() {
//   LockManager lock_mgr{};
//   TransactionManager txn_mgr{&lock_mgr};

//   table_oid_t oid = 0;
//   auto txn1 = txn_mgr.Begin();

//   /** Take X lock */
//   EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
//   CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

//   /** Upgrade X to S  fail*/
//   EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
//   // CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

//   /** Clean up */
//   txn_mgr.Commit(txn1);
//   CheckCommitted(txn1);
//   CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

//   delete txn1;
// }
// TEST(LockManagerTest, ENABLED_MyTableLockUpgradeTest1) { MyTableLockUpgradeTest1(); }  // NOLINT

}  // namespace bustub
