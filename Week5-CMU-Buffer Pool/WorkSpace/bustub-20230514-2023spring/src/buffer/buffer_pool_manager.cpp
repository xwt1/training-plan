//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  this->page_table_.clear();
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (!this->free_list_.empty()) {
    // 有空闲块,从空闲块读入一个frame
    page_id_t new_page_id = this->AllocatePage();
    frame_id_t free_frame_id = this->free_list_.front();
    this->free_list_.pop_front();

    Page *page = &this->pages_[free_frame_id];
    page->page_id_ = new_page_id;
    page_table_[new_page_id] = free_frame_id;
    page->pin_count_ = 1;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    *page_id = new_page_id;
    return (this->pages_ + free_frame_id);
  }
  // 如果没有空闲块了,就需要看能不能牺牲一块
  frame_id_t evict_frame_id = -233;
  if (this->replacer_->Evict(&evict_frame_id)) {
    if (this->pages_[evict_frame_id].is_dirty_) {
      this->disk_manager_->WritePage(this->pages_[evict_frame_id].GetPageId(), this->pages_[evict_frame_id].GetData());
      this->pages_[evict_frame_id].is_dirty_ = false;
    }

    page_id_t new_page_id = this->AllocatePage();

    this->page_table_.erase(this->pages_[evict_frame_id].page_id_);

    Page *page = &this->pages_[evict_frame_id];
    page->page_id_ = new_page_id;
    page_table_[new_page_id] = evict_frame_id;
    page->pin_count_ = 1;
    replacer_->RecordAccess(evict_frame_id);
    replacer_->SetEvictable(evict_frame_id, false);

    *page_id = new_page_id;

    return (this->pages_ + evict_frame_id);
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (this->page_table_.find(page_id) != this->page_table_.end()) {
    Page *page_to_find = (this->pages_ + this->page_table_[page_id]);
    page_to_find->pin_count_++;

    replacer_->RecordAccess(this->page_table_[page_id]);
    replacer_->SetEvictable(this->page_table_[page_id], false);

    return page_to_find;
  }
  // 接下来需要从disk中读取相应的page
  if (!this->free_list_.empty()) {
    frame_id_t free_frame_id = this->free_list_.front();
    this->free_list_.pop_front();
    Page *page = &this->pages_[free_frame_id];
    page->page_id_ = page_id;
    page_table_[page_id] = free_frame_id;
    // 从disk中读取数据
    this->disk_manager_->ReadPage(page_id, this->pages_[free_frame_id].GetData());
    // 记住这里不是pin_count =1!
    page->pin_count_++;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    return (this->pages_ + free_frame_id);
  }
  // 如果没有空闲块了,就需要看能不能牺牲一块
  frame_id_t evict_frame_id = -233;
  if (this->replacer_->Evict(&evict_frame_id)) {
    if (this->pages_[evict_frame_id].is_dirty_) {
      // 如果此时该页是脏页,就需要将其写回
      this->disk_manager_->WritePage(this->pages_[evict_frame_id].GetPageId(), this->pages_[evict_frame_id].GetData());
      this->pages_[evict_frame_id].is_dirty_ = false;
    }

    this->page_table_.erase(this->pages_[evict_frame_id].page_id_);
    Page *page = &this->pages_[evict_frame_id];
    page->page_id_ = page_id;
    page_table_[page_id] = evict_frame_id;
    // 从disk中读取数据
    this->disk_manager_->ReadPage(page_id, this->pages_[evict_frame_id].data_);
    page->pin_count_++;
    replacer_->RecordAccess(evict_frame_id);
    replacer_->SetEvictable(evict_frame_id, false);

    return (this->pages_ + evict_frame_id);
  }

  // 否则说明此时buffer pool中所有的page都是pinned状态
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = this->page_table_.find(page_id);
  if (it != this->page_table_.end()) {
    frame_id_t unpin_frame_id = it->second;
    if (this->pages_[unpin_frame_id].pin_count_ != 0) {
      // 此时说明该frame是可以正常unpin的
      this->pages_[unpin_frame_id].pin_count_--;
      if (this->pages_[unpin_frame_id].pin_count_ == 0) {
        // 如果此时的该frame的pin_count_的数量已经归零了,就把其设为evictable的
        this->replacer_->SetEvictable(unpin_frame_id, true);
      }
      this->pages_[unpin_frame_id].is_dirty_ |= is_dirty;
      return true;
      // 其余情况都是不正常的,返回false
    }
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = this->page_table_.find(page_id);
  if (it != this->page_table_.end()) {
    frame_id_t flush_frame_id = it->second;
    this->disk_manager_->WritePage(this->pages_[flush_frame_id].GetPageId(), this->pages_[flush_frame_id].GetData());
    this->pages_[flush_frame_id].is_dirty_ = false;
    return true;
  }
  // 找不到该page_id
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto &it : this->page_table_) {
    frame_id_t flush_frame_id = it.second;
    this->disk_manager_->WritePage(this->pages_[flush_frame_id].GetPageId(), this->pages_[flush_frame_id].GetData());
    this->pages_[flush_frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = this->page_table_.find(page_id);
  if (it != this->page_table_.end()) {
    frame_id_t delete_frame_id = it->second;
    if (this->pages_[delete_frame_id].GetPinCount() == 0) {
      // 存在该页并且pin的数量为0

      // 删除指定delete_frame_id的page块从page_table中
      this->page_table_.erase(page_id);
      // 停止追踪该块
      this->replacer_->Remove(delete_frame_id);
      // 清空该块
      this->pages_[delete_frame_id].ResetMemory();
      this->pages_[delete_frame_id].is_dirty_ = false;
      this->pages_[delete_frame_id].page_id_ = INVALID_PAGE_ID;
      this->pages_[delete_frame_id].pin_count_ = 0;
      // 将该块重新加回free_list
      this->free_list_.push_back(delete_frame_id);

      DeallocatePage(page_id);
      return true;
    }
  } else {
    // 不存在该页
    return true;
  }
  // 非法的情况
  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *fetch_page_frame = this->FetchPage(page_id);
  BasicPageGuard new_page_guard = BasicPageGuard(this, fetch_page_frame);
  return new_page_guard;
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *fetch_page_frame = this->FetchPage(page_id);
  ReadPageGuard new_read_page_guard = ReadPageGuard(this, fetch_page_frame);
  fetch_page_frame->RLatch();
  return new_read_page_guard;
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *fetch_page_frame = this->FetchPage(page_id);
  WritePageGuard new_write_page_guard = WritePageGuard(this, fetch_page_frame);
  fetch_page_frame->WLatch();
  return new_write_page_guard;
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *new_page_frame = this->NewPage(page_id);
  BasicPageGuard new_page_guard = BasicPageGuard(this, new_page_frame);
  return new_page_guard;
}

// void BufferPoolManager::DealWithComingPage(frame_id_t free_frame_id, page_id_t page_id) {
//   // 这里需要将原先的page_table_中指向evict_frame_id的page erase掉
//   page_id_t old_page_id = this->pages_[free_frame_id].page_id_;
//   // std::cout<<"free_frame_id: "<<free_frame_id<<std::endl;
//   // std::cout<<"old_page_id: "<<old_page_id<<std::endl;
//   // if(old_page_id != INVALID_PAGE_ID){
//   // std::cout<<this->page_table_[old_page_id]<<std::endl;
//   this->page_table_.erase(old_page_id);
//   // }
//   // std::cout<<this->page_table_[old_page_id]<<std::endl;
//   this->pages_[free_frame_id].ResetMemory();  // 清零操作
//   this->pages_[free_frame_id].pin_count_ = 1;
//   this->pages_[free_frame_id].page_id_ = page_id;
//   this->page_table_[page_id] = free_frame_id;
//   this->replacer_->RecordAccess(free_frame_id);         // 记录访问次数+1
//   this->replacer_->SetEvictable(free_frame_id, false);  // 设置当前不能访问的状态
// }

}  // namespace bustub
