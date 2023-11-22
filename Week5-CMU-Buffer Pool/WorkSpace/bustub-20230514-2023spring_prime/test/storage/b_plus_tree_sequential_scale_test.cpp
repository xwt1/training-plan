//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_sequential_scale_test.cpp
//
// Identification: test/storage/b_plus_tree_sequential_scale_test.cpp
//
// Copyright (c) 2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

/**
 * This test should be passing with your Checkpoint 1 submission.
 */
// TEST(BPlusTreeTests, ENABLED_ScaleTest) {  // NOLINT
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(30, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto *header_page = bpm->NewPage(&page_id);
//   (void)header_page;

//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
//   GenericKey<8> index_key;
//   RID rid;
//   // create transaction
//   auto *transaction = new Transaction(0);

//   // int64_t scale = 5;
//   int64_t scale = 5000;
//   std::vector<int64_t> keys;
//   for (int64_t key = 1; key < scale; key++) {
//     keys.push_back(key);
//   }

//   // randomized the insertion order
//   auto rng = std::default_random_engine{};
//   std::shuffle(keys.begin(), keys.end(), rng);
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid, transaction);
//   }
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     ASSERT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     ASSERT_EQ(rids[0].GetSlotNum(), value);
//   }

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete transaction;
//   delete bpm;
// }

// TEST(MyBPlusTreeTests, ENABLED_ScaleTest) {  // NOLINT
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(30, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto *header_page = bpm->NewPage(&page_id);
//   (void)header_page;

//   // create b+ tree
//   // BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 4);
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
//   GenericKey<8> index_key;
//   RID rid;
//   // create transaction
//   auto *transaction = new Transaction(0);

//   // int64_t scale = 5000;
//   // int64_t scale = 5000;
//   // std::vector<int64_t> keys = {1, 2, 3, 4, 5,5,5,5,6};
//   // std::vector<int64_t> keys = {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20};

//   std::vector<int64_t> keys = {1, 2, 3, 4, 5, 1, 2, 3, 4, 5};
//   // for (int64_t key = 1; key < scale; key++) {
//   //   keys.push_back(key);
//   // }

//   // // randomized the insertion order
//   // auto rng = std::default_random_engine{};
//   // std::shuffle(keys.begin(), keys.end(), rng);
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid, transaction);
//     std::cout << tree.DrawBPlusTree() << std::endl;
//   }
//   std::vector<RID> rids;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     ASSERT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     ASSERT_EQ(rids[0].GetSlotNum(), value);
//   }

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete transaction;
//   delete bpm;
// }

// TEST(BPlusTreeTests, ScaleTest) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);

//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 3,
//   5); GenericKey<8> index_key; RID rid;
//   // create transaction
//   auto *transaction = new Transaction(0);

//   // (wxx)  修改这里测试
//   int size = 50;

//   // output to file
//   // std::ofstream output("/WorkSpace/test/b_plus.txt");

//   std::vector<int64_t> keys(size);

//   std::iota(keys.begin(), keys.end(), 1);

//   std::random_device rd;
//   std::mt19937 g(rd());
//   std::shuffle(keys.begin(), keys.end(), g);

//   for (auto key : keys) {
//     // std::cout << key << std::endl;
//     // output << key << std::endl;
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid, transaction);
//   }

//   std::vector<RID> rids;

//   std::shuffle(keys.begin(), keys.end(), g);

//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   //  std::shuffle(keys.begin(), keys.end(), g);
//   // std::cout<<tree.DrawBPlusTree()<<std::endl;

//   // for(int i = 1;i<50;i+=2){

//   // }

//   for (auto key : keys) {
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key, transaction);
//     rids.clear();
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 0);
//     // std::cout<<tree.DrawBPlusTree()<<std::endl;
//   }

//   EXPECT_EQ(true, tree.IsEmpty());

//   bpm->UnpinPage(HEADER_PAGE_ID, true);

//   delete transaction;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }

// 随机测试,针对mixTest
TEST(BPlusTreeTests2, ScaleTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 3, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // (wxx)  修改这里测试
  int size = 10000;

  // output to file
  // std::ofstream output("/WorkSpace/test/b_plus.txt");

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  std::cout << "插入开始" << std::endl;
  for (auto key : keys) {
    std::cout << key << std::endl;
    // output << key << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::cout << "插入结束" << std::endl;
  std::vector<RID> rids;

  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  //  std::shuffle(keys.begin(), keys.end(), g);
  // std::cout<<tree.DrawBPlusTree()<<std::endl;

  for (int64_t i = 1; i < size; i += 2) {
    int64_t now_key = i;
    index_key.SetFromInteger(now_key);
    tree.Remove(index_key, transaction);
    rids.clear();
  }

  for (auto iters = tree.Begin(); iters != tree.End(); ++iters) {
    const auto &pair = *iters;
    std::cout << "key= " << pair.first << std::endl;
  }

  // for (auto key : keys) {
  //   index_key.SetFromInteger(key);
  //   tree.Remove(index_key, transaction);
  //   rids.clear();
  //   tree.GetValue(index_key, &rids);
  //   EXPECT_EQ(rids.size(), 0);
  //   // std::cout<<tree.DrawBPlusTree()<<std::endl;
  // }

  // EXPECT_EQ(true, tree.IsEmpty());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// // 随机测试,针对mixTest
// TEST(BPlusTreeTests2, ScaleTest) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);

//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 3,
//   5); GenericKey<8> index_key; RID rid;
//   // create transaction
//   auto *transaction = new Transaction(0);

//   // (wxx)  修改这里测试
//   int size = 10;

//   // output to file
//   // std::ofstream output("/WorkSpace/test/b_plus.txt");

//   std::vector<int64_t> keys={5,6,7,1,3,2,8,4,9,10};

//   std::iota(keys.begin(), keys.end(), 1);

//   std::random_device rd;
//   std::mt19937 g(rd());
//   std::shuffle(keys.begin(), keys.end(), g);
//   std::cout << "插入开始" << std::endl;
//   for (auto key : keys) {
//     std::cout << key << std::endl;
//     // output << key << std::endl;
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key >> 32), value);
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid, transaction);
//   }
//   std::cout << "插入结束" << std::endl;
//   std::vector<RID> rids;

//   std::shuffle(keys.begin(), keys.end(), g);

//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   //  std::shuffle(keys.begin(), keys.end(), g);
//   // std::cout<<tree.DrawBPlusTree()<<std::endl;

//   for(int64_t i = 1;i<size;i+=2){
//     int64_t now_key = i;
//     index_key.SetFromInteger(now_key);
//     tree.Remove(index_key, transaction);
//     rids.clear();
//   }

//   for(auto iters = tree.Begin();iters!= tree.End();++iters){
//     const auto &pair = *iters;
//     std::cout<<"key= "<<pair.first<<std::endl;
//   }

//   // for (auto key : keys) {
//   //   index_key.SetFromInteger(key);
//   //   tree.Remove(index_key, transaction);
//   //   rids.clear();
//   //   tree.GetValue(index_key, &rids);
//   //   EXPECT_EQ(rids.size(), 0);
//   //   // std::cout<<tree.DrawBPlusTree()<<std::endl;
//   // }

//   // EXPECT_EQ(true, tree.IsEmpty());

//   bpm->UnpinPage(HEADER_PAGE_ID, true);

//   delete transaction;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }

}  // namespace bustub
