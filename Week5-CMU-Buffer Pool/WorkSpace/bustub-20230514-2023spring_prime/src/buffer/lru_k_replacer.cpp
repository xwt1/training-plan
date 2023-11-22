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

auto LRUKNode::GetHistory() const -> const std::list<size_t> & { return this->history_; }

void LRUKNode::SetHistory(const std::list<size_t> &history_) { this->history_ = history_; }

void LRUKNode::PushFrontHistory(const size_t &value) { this->history_.push_front(value); }

void LRUKNode::PopBackHistory() { this->history_.pop_back(); }

void LRUKNode::ClearHistory() { this->history_.clear(); }

void LRUKNode::SetK(const size_t &k) { this->k_ = k; }

auto LRUKNode::GetK() const -> const size_t & { return this->k_; }

void LRUKNode::SetFid(const frame_id_t &fid_) { this->fid_ = fid_; }

auto LRUKNode::GetFid() const -> const frame_id_t & { return this->fid_; }

auto LRUKNodeCompare::operator()(const LRUKNode &A, const LRUKNode &B) const -> bool {
  // 这里History每次都是从头部插入,所以最老的记录永远在尾部,
  // 我们需要让最需要被evict的frame在set的begin位置
  if (A.GetHistory().size() < A.GetK()) {
    if (B.GetHistory().size() < B.GetK()) {
      // 这里越先前的记录越小,说明越是要被evict的页面
      return A.GetHistory().back() < B.GetHistory().back();
    }
    return true;
  }
  if (B.GetHistory().size() < B.GetK()) {
    return false;
  }
  // 因为实际上是用current_timestamp - A.history_.back()
  // 与current_timestamp - B.history_.back()比较,实际上只要比较
  // A.history_.back() 与B.history_.back()就可以了
  return A.GetHistory().back() < B.GetHistory().back();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  this->latch_.lock();
  for (size_t i = 0; i < num_frames; i++) {
    this->node_store_[i] = std::make_shared<LRUKNode>(k, static_cast<frame_id_t>(i));
    this->is_evictable_[i] = false;
  }
  this->latch_.unlock();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  this->latch_.lock();
  if (this->node_evict_.empty()) {
    this->latch_.unlock();
    return false;
  }
  auto most_evict_ptr = this->node_evict_.begin();
  frame_id_t evict_frame_id = (most_evict_ptr)->GetFid();
  if (!this->is_evictable_[evict_frame_id]) {
    *frame_id = -2333;
    this->latch_.unlock();
    return true;
  }
  *frame_id = evict_frame_id;

  this->node_evict_.erase(most_evict_ptr);
  this->node_store_[*frame_id]->ClearHistory();
  this->latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  this->latch_.lock();
  this->current_timestamp_++;
  LRUKNode temp_node_store_lruk_node = *(this->node_store_[frame_id]);
  // 我到现在都想不明白，为什么要加下面这句话？？？为啥要置true啊？
  if (this->node_store_[frame_id]->GetHistory().empty()) {
    this->is_evictable_[frame_id] = true;
  }
  while (this->node_store_[frame_id]->GetHistory().size() >= this->node_store_[frame_id]->GetK()) {
    this->node_store_[frame_id]->PopBackHistory();
  }
  this->node_store_[frame_id]->PushFrontHistory(this->current_timestamp_);
  if (this->is_evictable_[frame_id]) {
    // 代表是在可以被牺牲的队列中的.
    this->ReplaceOldInNodeEvict(temp_node_store_lruk_node, frame_id);
  }
  this->latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  this->latch_.lock();
  if (set_evictable) {
    if (!this->is_evictable_[frame_id]) {
      this->is_evictable_[frame_id] = true;
      this->node_evict_.insert(*(this->node_store_[frame_id]));
    }
  } else {
    if (this->is_evictable_[frame_id]) {
      this->is_evictable_[frame_id] = false;
      auto temp_evict_lruk_node_ptr = this->node_evict_.find(*(this->node_store_[frame_id]));
      if (temp_evict_lruk_node_ptr != this->node_evict_.end()) {
        this->node_evict_.erase(temp_evict_lruk_node_ptr);
      }
    }
  }
  this->latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  this->latch_.lock();
  LRUKNode temp_evict_lruk_node = *(this->node_store_[frame_id]);
  this->node_store_[frame_id]->ClearHistory();
  this->node_evict_.erase(temp_evict_lruk_node);
  this->latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  this->latch_.lock();
  auto ret = static_cast<size_t>(this->node_evict_.size());
  this->latch_.unlock();
  return ret;
}

void LRUKReplacer::ReplaceOldInNodeEvict(const LRUKNode &temp_node_store_lruk_node, frame_id_t frame_id) {
  auto temp_evict_lruk_node_ptr = this->node_evict_.find(temp_node_store_lruk_node);
  if (temp_evict_lruk_node_ptr != this->node_evict_.end()) {
    // 把旧的Node删除
    this->node_evict_.erase(temp_evict_lruk_node_ptr);
    // 添加新的Node
    this->node_evict_.insert(*(this->node_store_[frame_id]));
  } else {
    this->node_evict_.insert(*(this->node_store_[frame_id]));
  }
}

}  // namespace bustub
