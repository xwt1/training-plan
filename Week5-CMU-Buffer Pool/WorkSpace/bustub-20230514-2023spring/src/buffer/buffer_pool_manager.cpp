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
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }



auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  if(!this->free_list_.empty()){
    //上锁
    this->latch_.lock();

    //有空闲块,从空闲块读入一个frame
    page_id_t new_page_id = this->AllocatePage();
    frame_id_t free_frame_id = this->free_list_.front();
    this->free_list_.pop_front();


    // this->pages_[free_frame_id].ResetMemory();  //清零操作
    // this->pages_[free_frame_id].pin_count_ = 1;    
    // this->pages_[free_frame_id].page_id_ = new_page_id;
    // this->page_table_[new_page_id] = free_frame_id;
    // this->replacer_->RecordAccess(free_frame_id); //记录访问次数+1
    // this->replacer_->SetEvictable(free_frame_id,false); //设置当前不能访问的状态
    this->DealWithComingPage(free_frame_id,new_page_id);

    *page_id = new_page_id;
    this->latch_.unlock();
    return (this->pages_ + free_frame_id);
  }else{
    //如果没有空闲块了,就需要看能不能牺牲一块
    frame_id_t evict_frame_id = -233; 
    if(this->replacer_->Evict(&evict_frame_id)){
      this->latch_.lock();
      if(this->pages_[evict_frame_id].is_dirty_){
        //如果此时该页是脏页,就需要将其写回
        this->disk_manager_->WritePage(this->pages_[evict_frame_id].page_id_,this->pages_[evict_frame_id].data_);
        // DiskManager::WritePage(this->pages_[evict_frame_id].page_id_,this->pages_[evict_frame_id].data_);
      }

      page_id_t new_page_id = this->AllocatePage();

      // this->pages_[evict_frame_id].ResetMemory();  //清零操作
      // this->pages_[evict_frame_id].pin_count_ = 1;
      // this->pages_[evict_frame_id].page_id_ = new_page_id;
      // this->page_table_[new_page_id] = evict_frame_id;
      // this->replacer_->RecordAccess(evict_frame_id); //记录访问次数+1
      // this->replacer_->SetEvictable(evict_frame_id,false); //设置当前不能访问的状态
      this->DealWithComingPage(evict_frame_id,new_page_id);
      *page_id = new_page_id;
      this->latch_.unlock();

      return (this->pages_ + evict_frame_id);
    }
  }
  //否则说明此时buffer pool中所有的page都是pinned状态
  return nullptr;   
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // bool is_page_in_buffer = false; 
  Page *page_to_find = nullptr;
  this->latch_.lock();
  for(size_t i = 0;i < this->pool_size_;i++){
    if(this->pages_[i].page_id_ == page_id){
      // is_page_in_buffer = true;
      page_to_find = (this->pages_ + i);
      break;
    }
  }
  this->latch_.unlock();
  // printf("ok\n");
  if(page_to_find != nullptr){
    this->latch_.lock();

    page_to_find->pin_count_++;
    this->latch_.unlock();
    return page_to_find;
  }
  //接下来需要从disk中读取相应的page
  if(!this->free_list_.empty()){
    this->latch_.lock();
    frame_id_t free_frame_id = this->free_list_.front();
    this->free_list_.pop_front();

    // this->pages_[free_frame_id].ResetMemory();  //清零操作
    // this->pages_[free_frame_id].pin_count_ = 1;
    // this->pages_[free_frame_id].page_id_ = page_id;
    // this->page_table_[page_id] = free_frame_id;
    // this->replacer_->RecordAccess(free_frame_id); //记录访问次数+1
    // this->replacer_->SetEvictable(free_frame_id,false); //设置当前不能访问的状态
    this->DealWithComingPage(free_frame_id,page_id);


    //从disk中读取数据
    this->disk_manager_->ReadPage(page_id,this->pages_[free_frame_id].data_);

    this->latch_.unlock();
    return (this->pages_ + free_frame_id);
  }else{

    //如果没有空闲块了,就需要看能不能牺牲一块
    frame_id_t evict_frame_id = -233; 
    if(this->replacer_->Evict(&evict_frame_id)){
      this->latch_.lock();
      if(this->pages_[evict_frame_id].is_dirty_){
        //如果此时该页是脏页,就需要将其写回
        this->disk_manager_->WritePage(this->pages_[evict_frame_id].page_id_,this->pages_[evict_frame_id].data_);
      }

      // this->pages_[evict_frame_id].ResetMemory();  //清零操作
      // this->pages_[evict_frame_id].pin_count_ = 1;
      // this->pages_[evict_frame_id].page_id_ = page_id;
      // this->page_table_[page_id] = evict_frame_id;
      // this->replacer_->RecordAccess(evict_frame_id); //记录访问次数+1
      // this->replacer_->SetEvictable(evict_frame_id,false); //设置当前不能访问的状态
      this->DealWithComingPage(evict_frame_id,page_id);

      //从disk中读取数据
      this->disk_manager_->ReadPage(page_id,this->pages_[evict_frame_id].data_);

      this->latch_.unlock();

      return (this->pages_ + evict_frame_id);
    }
  }
  //否则说明此时buffer pool中所有的page都是pinned状态
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unordered_map <int,int>::iterator it = this->page_table_.find(page_id);
  if(it != this->page_table_.end()){
    frame_id_t unpin_frame_id = it->second;
    if(this->pages_[unpin_frame_id].pin_count_){
      this->latch_.lock();

      //此时说明该frame是可以正常unpin的
      this->pages_[unpin_frame_id].pin_count_--;
      if(!this->pages_[unpin_frame_id].pin_count_){
        //如果此时的该frame的pin_count_的数量已经归零了,就把其设为evictable的
        this->replacer_->SetEvictable(unpin_frame_id, true);
      }else{
        //保险起见,多这一步操作,设置为unevictable的
        this->replacer_->SetEvictable(unpin_frame_id, false);
      }
      this->pages_[unpin_frame_id].is_dirty_= (is_dirty)==true?true:false;

      this->latch_.unlock();

      return true;
      //其余情况都是不正常的,返回false
    }
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unordered_map <int,int>::iterator it = this->page_table_.find(page_id);
  if(it != this->page_table_.end()){
    this->latch_.lock();
    frame_id_t flush_frame_id = it->second;
    this->disk_manager_->WritePage(this->pages_[flush_frame_id].page_id_,this->pages_[flush_frame_id].data_);
    this->pages_[flush_frame_id].is_dirty_ = false;
    this->latch_.unlock();
    return true;
  }
  //找不到该page_id
  return false; 
}

void BufferPoolManager::FlushAllPages() {
  this->latch_.lock();
  for(auto it = this->page_table_.begin();it != this->page_table_.end();it++){
    frame_id_t flush_frame_id = it->second;
    this->disk_manager_->WritePage(this->pages_[flush_frame_id].page_id_,this->pages_[flush_frame_id].data_);
    this->pages_[flush_frame_id].is_dirty_ = false;
  }
  this->latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::unordered_map <int,int>::iterator it = this->page_table_.find(page_id);
  if(it != this->page_table_.end()){
    frame_id_t delete_frame_id = it->second;
    if(!this->pages_[delete_frame_id].pin_count_){
      this->latch_.lock();
      //存在该页并且pin的数量为0

      //删除指定delete_frame_id的page块从page_table中
      this->page_table_.erase(page_id);
      //停止追踪该块
      this->replacer_->Remove(delete_frame_id);
      //清空该块
      this->pages_[delete_frame_id].ResetMemory();
      //将该块重新加回free_list
      this->free_list_.push_back(delete_frame_id);
      
      DeallocatePage(page_id);
      this->latch_.unlock();
      return true;
    }
  }else{
    //不存在该页
    return true;
  }
  //非法的情况
  return false; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  return next_page_id_++; 
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

void BufferPoolManager::DealWithComingPage(frame_id_t free_frame_id,page_id_t page_id){
  this->pages_[free_frame_id].ResetMemory();  //清零操作
  this->pages_[free_frame_id].pin_count_ = 1;
  this->pages_[free_frame_id].page_id_ = page_id;
  this->page_table_[page_id] = free_frame_id;
  this->replacer_->RecordAccess(free_frame_id); //记录访问次数+1
  this->replacer_->SetEvictable(free_frame_id,false); //设置当前不能访问的状态
}

}  // namespace bustub
