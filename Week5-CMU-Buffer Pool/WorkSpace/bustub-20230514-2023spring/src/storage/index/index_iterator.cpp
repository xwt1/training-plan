/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&page_guard_, ReadPageGuard &&head_guard_,
//                                   int index)
//     : bpm_(bpm), page_guard_(std::move(page_guard_)), head_guard_(std::move(head_guard_)), index_(index) {
//   if (bpm_ != nullptr && index != -233) {
//     this->page_id_ = this->page_guard_.PageId();
//   } else {
//     this->page_id_ = INVALID_PAGE_ID;
//   }
// }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&page_guard_, int index)
    : bpm_(bpm), page_guard_(std::move(page_guard_)), index_(index) {
  if (bpm_ != nullptr && index != -233) {
    this->page_id_ = this->page_guard_.PageId();
  } else {
    this->page_id_ = INVALID_PAGE_ID;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return this->page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf_page = this->page_guard_.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return leaf_page->KeyValueAt(this->index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (!this->IsEnd()) {
    auto leaf = this->page_guard_.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    this->index_++;
    if (this->index_ == leaf->GetSize()) {
      if (leaf->GetNextPageId() == INVALID_PAGE_ID) {
        bpm_ = nullptr;
        this->index_ = -233;
        this->page_guard_.Drop();
        // this->head_guard_.Drop();
        this->page_id_ = INVALID_PAGE_ID;
        return *this;
      }
      this->page_guard_ = bpm_->FetchPageRead(leaf->GetNextPageId());
      this->page_id_ = this->page_guard_.PageId();
      // 代表读到第一个
      this->index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
