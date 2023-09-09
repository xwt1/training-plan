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

LRUKNode::LRUKNode(size_t k_,frame_id_t fid_):k_(k_),fid_(fid_){
    this->history = std::list <size_t>();
}

const std::list<size_t> &LRUKNode::getHistory() const { return this->history_; }

void LRUKNode::setHistory(const std::list<size_t> &history_) { this->history_ = history_; }

void LRUKNode::pushFrontHistory(const size_t &value) { this->history_.push_front(value); }

void LRUKNode::popBackHistory() { this->history_.pop_back(); }

void LRUKNode::setK_(const size_t &k) { this->k_ = k; }

const size_t &LRUKNode::getK_() const { return this->k_; }

void LRUKNode::setFid_(const frame_id_t &fid_) { this->fid_ = fid_; }

const frame_id_t &LRUKNode::getFid_() const { return this->fid_; }

void LRUKNode::setIsEvictable_(const bool &is_evictable_){
    this->is_evictable_ = is_evictable_;
}

const bool & LRUKNode::getIsEvictable_() const{
    return this->is_evictable_;
}

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
  for (size_t i = 0; i < num_frames; i++) {
    this->node_store_[i] = std::make_shared<LRUKNode>(k, (frame_id_t)i);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if(this->node_evict_.empty())   return false;
    else{
        auto most_evict_ptr = this->node_evict_.first();
        frame_id_t evict_frame_id = (*most_evict_ptr)->getFid_();
        frame_id = &evict_frame_id;
        //添加代码


        // this->node_store_[evict_frame_id]->setIsEvictable_(false);
        //可能要清除history？那么应该接下来就要调用Remove
        return true;
    } 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    this->current_timestamp_++;
    auto temp_evict_LRUKNode
    LRUKNode temp_node_store_LRUKNode = *(this->node_store_[frame_id]);
    while(this->node_store_[frame_id]->getHistory().size() >= this->node_store_[frame_id]->getK_()){
        this->node_store_[frame_id]->popBackHistory();
    }
    this->node_store_[frame_id]->pushFrontHistory(this->current_timestamp_);
    //下面的代码块可以单独封装一个函数出来
    auto temp_evict_LRUKNode = this->node_evict_.find(temp_node_store_LRUKNode);
    if(temp_evict_LRUKNode != this->node_evict_.end()){
        this->node_evict_.erase(temp_evict_LRUKNode);
    }
    this->node_evict_.insert(*(this->node_store_[frame_id]));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(set_evictable){
        /*
                              node_evict_=0      node_evict_=1
            node_store_ =0    不可能发生            可能发生
            node_store_ =1  
        */
        LRUKNode temp_evict_LRUKNode = *(this->node_store_[frame_id]);
        auto temp_evict_LRUKNode_ptr = node_evict_.find(temp_evict_LRUKNode);
        if(temp_evict_LRUKNode_ptr== node_evict_.end()){ 
            //说明此时node_store_中该frame是unevictable的，
            //而node_evict_中不包含unevictable的frame。
            if(temp_evict_LRUKNode.getIsEvictable_()){
                //添加代码
                // if()
            }else{
        
            }
        }else{
            //此时node_store_和node_evict_存储的都是evictable的。不需要设置

        }
    }else{
        
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
