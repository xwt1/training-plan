//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  // IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&page_guard_, ReadPageGuard &&head_guard_, int index = -233);
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&page_guard_, int index = -233);

  auto operator=(IndexIterator &&that) noexcept -> IndexIterator&;

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (this->page_id_ == INVALID_PAGE_ID) {
      // 说明是一个不合法的page，则如果对方也是不合法的,则相等
      return itr.page_id_ == INVALID_PAGE_ID;
    }
    // 合法的page必须page_id一样,下标一样
    return this->index_ == itr.index_ && this->page_id_ == itr.page_id_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    // return !this->operator==(itr);
    if (this->page_id_ == INVALID_PAGE_ID) {
      return itr.page_id_ != INVALID_PAGE_ID;
    }
    return this->index_ != itr.index_ || this->page_id_ != itr.page_id_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  ReadPageGuard page_guard_;
  // ReadPageGuard head_guard_;
  int index_;
  // 当前的page_id,当为End时会体现出作用
  page_id_t page_id_;
};

}  // namespace bustub
