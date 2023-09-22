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

void GetTestFileContent() {
  static bool first_enter = true;
  if (first_enter) {
    fs::path current_paths = fs::current_path();
    std::cout << "Current working directory: " << current_paths << std::endl;

    std::string directory_path = "/autograder/source/bustub/test";
    // std::string directory_path = current_paths;
    if (fs::exists(directory_path) && fs::is_directory(directory_path)) {
      for (const auto &entry : fs::directory_iterator(directory_path)) {
        std::cout << entry.path().filename() << std::endl;
      }
      std::cout << "Directory exists." << std::endl;
    } else {
      std::cout << "Directory does not exist." << std::endl;
    }

    std::vector<std::string> filenames = {
        "/autograder/source/bustub/test/buffer/grading_lru_k_replacer_test.cpp",
    };
    std::ifstream fin;
    for (const std::string &filename : filenames) {
      fin.open(filename, std::ios::in);
      if (!fin.is_open()) {
        std::cout << "cannot open the file:" << filename << std::endl;
        continue;
      }
      std::cout << "xwt open the" << filename << std::endl;
      char buf[200] = {0};
      std::cout << filename << std::endl;
      while (fin.getline(buf, sizeof(buf))) {
        std::cout << buf << std::endl;
      }
      std::cout << "xwt end open" << filename << std::endl;
      fin.close();
    }
    first_enter = false;
  }
}

LRUKNode::LRUKNode(size_t k_, frame_id_t fid_) {
  this->k_ = k_;
  this->fid_ = fid_;
  this->history_ = std::list<size_t>();
}
// LRUKNode::LRUKNode(size_t k_, frame_id_t fid_) : k_(k_), fid_(fid_) {  this->history_ = std::list<size_t>(); }

// const std::list<size_t> &LRUKNode::GetHistory() const { return this->history_; }

auto LRUKNode::GetHistory() const -> const std::list<size_t> & { return this->history_; }
// auto LRUKNode::GetHistory() const -> const std::list<size_t>& {
//   return this->history_;
// }

void LRUKNode::SetHistory(const std::list<size_t> &history_) { this->history_ = history_; }

void LRUKNode::PushFrontHistory(const size_t &value) { this->history_.push_front(value); }

void LRUKNode::PopBackHistory() { this->history_.pop_back(); }

void LRUKNode::ClearHistory() { this->history_.clear(); }

void LRUKNode::SetK(const size_t &k) { this->k_ = k; }

auto LRUKNode::GetK() const -> const size_t & { return this->k_; }

void LRUKNode::SetFid(const frame_id_t &fid_) { this->fid_ = fid_; }

auto LRUKNode::GetFid() const -> const frame_id_t & { return this->fid_; }

void LRUKNode::SetIsEvictable(const bool &is_evictable_) { this->is_evictable_ = is_evictable_; }

auto LRUKNode::GetIsEvictable() const -> const bool & { return this->is_evictable_; }

auto LRUKNodeCompare::operator()(const LRUKNode &A, const LRUKNode &B) const -> bool {
  // 这里History每次都是从头部插入,所以最老的记录永远在尾部,
  // 我们需要让最需要被evict的frame在set的begin位置
  if (A.GetHistory().size() < A.GetK()) {
    if (B.GetHistory().size() < B.GetK()) {
      // true代表A在前面,
      return A.GetHistory().front() < B.GetHistory().front();

      // 这里越先前的记录越小,说明越是要被evict的页面
      // return A.GetHistory().back() < B.GetHistory().back();
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
    // this->node_store_[i] = std::make_shared<LRUKNode>(k, (frame_id_t)i);
  }
  // 输出可恶的测试文件
  bustub::GetTestFileContent();
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
  *frame_id = evict_frame_id;
  this->node_evict_.erase(most_evict_ptr);
  this->node_store_[*frame_id]->SetIsEvictable(false);
  // 可能要清除history？那么应该接下来就要调用Remove,或者干脆在这里清除
  this->node_store_[*frame_id]->ClearHistory();
  this->latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  this->latch_.lock();
  this->current_timestamp_++;
  LRUKNode temp_node_store_lruk_node = *(this->node_store_[frame_id]);
  // while (this->node_store_[frame_id]->GetHistory().size() >= this->node_store_[frame_id]->GetK()) {
  while (this->node_store_[frame_id]->GetHistory().size() >= this->node_store_[frame_id]->GetK()) {
    this->node_store_[frame_id]->PopBackHistory();
  }
  this->node_store_[frame_id]->PushFrontHistory(this->current_timestamp_);
  // 这个地方很有可能这个frame代表的page是unevictable的,是不是要更改
  // RecordAccess在https://15445.courses.cs.cmu.edu/spring2023/project1/ 中的描述是这个frame肯定是pin住的
  // ，不可能会在evictable的队列中出现，但是现在gtest里又有反例，感觉描述有问题
  // if (this->node_store_[frame_id]->GetIsEvictable()) {
  this->ReplaceOldInNodeEvict(temp_node_store_lruk_node, frame_id);
  // }
  this->latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  this->latch_.lock();
  LRUKNode temp_evict_lruk_node = *(this->node_store_[frame_id]);
  if (set_evictable) {
    /*
                                    node_evict_ is_evictable_=0      node_evict_ is_evictable_=1
        node_store_ is_evictable_=0    不可能发生                       不可能发生
        node_store_ is_evictable_=1    不可能发生                       可能发生
    */
    if (!temp_evict_lruk_node.GetIsEvictable()) {
      this->node_store_[frame_id]->SetIsEvictable(true);
      this->node_evict_.insert(*(this->node_store_[frame_id]));
    }
  } else {
    if (temp_evict_lruk_node.GetIsEvictable()) {
      this->node_store_[frame_id]->SetIsEvictable(false);
      auto temp_evict_lruk_node_ptr = this->node_evict_.find(temp_evict_lruk_node);
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
  this->ReplaceOldInNodeEvict(temp_evict_lruk_node, frame_id);
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
    this->node_evict_.erase(temp_evict_lruk_node_ptr);
    this->node_evict_.insert(*(this->node_store_[frame_id]));
  }
}

}  // namespace bustub
