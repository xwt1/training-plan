/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <stack>
#include <string>
// #include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  // 保存当前父亲结点page的指针以及要向下搜寻的page在数组中的下标,由于BPlusTreePage删除了析构函数,
  // 可以认为在栈销毁的时候,不会调用BPlusTreePage的析构函数去销毁Page的空间
  std::stack<std::pair<BPlusTreePage *,int>> parent_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

  int insert_num_{0};

  int read_num_{0};

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * 插入
   * 
  */

  /*
    插入一个value值在pos位置上，数组从原先的pos开始向右移动一格
  */
  void InsertLeafAValue(LeafPage *pages, const MappingType &value, int pos);

  /*
    插入一个value值在pos位置上，数组从原先的pos开始向右移动一格
  */
  void InsertInternalAValue(InternalPage *pages, const std::pair<KeyType, page_id_t> &value, int pos);

  /*
    处理内部结点的分裂
  */
  void DealWithInternalSplit(InternalPage *pages, int key_index, const KeyType &key,
                             const std::pair<page_id_t, page_id_t> &left_right_son, KeyType &key_to_push,
                             std::pair<page_id_t, page_id_t> &left_right_son_to_push);
  /*
    处理叶子结点的分裂
  */
  void DealWithLeafSplit(LeafPage *pages, int key_index, const KeyType &key, const ValueType &value,
                         KeyType &key_to_push, std::pair<page_id_t, page_id_t> &left_right_son_to_push);

  /*
    处理根节点分裂
  */
  // void DealWithRoot(page_id_t * new_root_id,const Context& ctx,
  // const KeyType &last_split,const std::pair<page_id_t,page_id_t> &left_right_id) ;

  /*
    构建一个新的page
      page_id是新建的page的id,page_type是page的类型
  */
  void BuildNewPage(page_id_t *page_id, IndexPageType page_type);


  /**
   * 删除
   * 
  */
  
  /**
   * 尝试能不能借用一个兄弟叶子结点的值
   * page和page_id是要处理的叶子结点和叶子结点的编号，parent.first是父节点，parent.second是要处理的叶子结点的编号在父节点数组中的下标
   * 返回值表示有没有发生合并,true代表发生了合并,false代表没有
  */
  auto BorrowOrCoalesceLeafPage(LeafPage * page,page_id_t page_id,std::pair<BPlusTreePage *,int> parent) -> bool;

  /**
   * 尝试能不能借用一个兄弟内部结点的值
   * page和page_id是要处理的内部结点和内部结点的编号，parent.first是父节点，parent.second是要处理的内部结点的编号在父节点数组中的下标
   * 返回值表示有没有发生合并,true代表发生了合并,false代表没有
  */
  auto BorrowOrCoalesceInternalPage(InternalPage * page, page_id_t page_id,std::pair<BPlusTreePage *,int> parent) -> bool;

  // void DeleteAValueFrom

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printalbe B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
