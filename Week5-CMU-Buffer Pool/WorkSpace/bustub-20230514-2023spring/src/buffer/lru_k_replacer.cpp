//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k_, frame_id_t fid_) {
  this->k_ = k_;
  this->fid_ = fid_;
  this->history_ = std::list<size_t>();
}
// LRUKNode::LRUKNode(size_t k_, frame_id_t fid_) : k_(k_), fid_(fid_) {  this->history_ = std::list<size_t>(); }

const std::list<size_t> &LRUKNode::getHistory() const { return this->history_; }

void LRUKNode::setHistory(const std::list<size_t> &history_) { this->history_ = history_; }

void LRUKNode::pushFrontHistory(const size_t &value) { this->history_.push_front(value); }

void LRUKNode::popBackHistory() { this->history_.pop_back(); }

void LRUKNode::clearHistory() { this->history_.clear(); }

void LRUKNode::setK_(const size_t &k) { this->k_ = k; }

const size_t &LRUKNode::getK_() const { return this->k_; }

void LRUKNode::setFid_(const frame_id_t &fid_) { this->fid_ = fid_; }

const frame_id_t &LRUKNode::getFid_() const { return this->fid_; }

void LRUKNode::setIsEvictable_(const bool &is_evictable_) { this->is_evictable_ = is_evictable_; }

const bool &LRUKNode::getIsEvictable_() const { return this->is_evictable_; }

bool LRUKNodeCompare::operator()(const LRUKNode &A, const LRUKNode &B) const {
  if (A.getHistory().size() < A.getK_()) {
    if (B.getHistory().size() < B.getK_()) {
      return A.getHistory().front() < B.getHistory().front();
    } else {
      return true;
    }
  } else {
    if (B.getHistory().size() < B.getK_()) {
      return false;
    } else {
      // 因为实际上是用current_timestamp - A.history_.back()
      // 与current_timestamp - this->history_.back()比较,实际上只要比较
      // A.history_.back() 与this->history_.back()就可以了
      return A.getHistory().back() < B.getHistory().back();
    }
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  this->latch_.lock();
  for (size_t i = 0; i < num_frames; i++) {
    this->node_store_[i] = std::make_shared<LRUKNode>(k, (frame_id_t)i);
  }
  this->latch_.unlock();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  this->latch_.lock();
  if (this->node_evict_.empty()) {
    this->latch_.unlock();
    return false;
  } else {
    auto most_evict_ptr = this->node_evict_.begin();
    frame_id_t evict_frame_id = (most_evict_ptr)->getFid_();
    *frame_id = evict_frame_id;
    this->node_evict_.erase(most_evict_ptr);
    this->node_store_[*frame_id]->setIsEvictable_(false);
    // 可能要清除history？那么应该接下来就要调用Remove,或者干脆在这里清除
    this->node_store_[*frame_id]->clearHistory();
    this->latch_.unlock();
    return true;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  this->latch_.lock();
  this->current_timestamp_++;
  LRUKNode temp_node_store_LRUKNode = *(this->node_store_[frame_id]);
  while (this->node_store_[frame_id]->getHistory().size() >= this->node_store_[frame_id]->getK_()) {
    this->node_store_[frame_id]->popBackHistory();
  }
  this->node_store_[frame_id]->pushFrontHistory(this->current_timestamp_);
  this->ReplaceOldInNode_Evict_(temp_node_store_LRUKNode, frame_id);
  this->latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  this->latch_.lock();
  LRUKNode temp_evict_LRUKNode = *(this->node_store_[frame_id]);
  if (set_evictable) {
    /*
                                    node_evict_ is_evictable_=0      node_evict_ is_evictable_=1
        node_store_ is_evictable_=0    不可能发生                       不可能发生
        node_store_ is_evictable_=1    不可能发生                       可能发生
    */
    if (!temp_evict_LRUKNode.getIsEvictable_()) {
      this->node_store_[frame_id]->setIsEvictable_(true);
      this->node_evict_.insert(*(this->node_store_[frame_id]));
    }
  } else {
    if (temp_evict_LRUKNode.getIsEvictable_()) {
      this->node_store_[frame_id]->setIsEvictable_(false);
      auto temp_evict_LRUKNode_ptr = this->node_evict_.find(temp_evict_LRUKNode);
      if (temp_evict_LRUKNode_ptr != this->node_evict_.end()) {
        this->node_evict_.erase(temp_evict_LRUKNode_ptr);
      }
    }
  }
  this->latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  this->latch_.lock();
  LRUKNode temp_evict_LRUKNode = *(this->node_store_[frame_id]);
  this->node_store_[frame_id]->clearHistory();
  this->ReplaceOldInNode_Evict_(temp_evict_LRUKNode, frame_id);
  this->latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  this->latch_.lock();
  size_t ret = (size_t)this->node_evict_.size();
  this->latch_.unlock();
  return ret;
}

void LRUKReplacer::ReplaceOldInNode_Evict_(LRUKNode temp_node_store_LRUKNode, frame_id_t frame_id) {
  auto temp_evict_LRUKNode_ptr = this->node_evict_.find(temp_node_store_LRUKNode);
  if (temp_evict_LRUKNode_ptr != this->node_evict_.end()) {
    this->node_evict_.erase(temp_evict_LRUKNode_ptr);
    this->node_evict_.insert(*(this->node_store_[frame_id]));
  }
}

}  // namespace bustub
