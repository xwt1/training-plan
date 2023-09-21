#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  // auto drop_frame_id = this->bpm_->page_table_[this->page->page_id_];
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  }
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 这里我直接drop原先的页面,感觉不太对,他的注释光说要考虑有啥动作需要考虑，
  // 当原先的页面被覆盖的时候。
  // 但是目前动作貌似就drop一个，只好drop了,就很奇怪为啥这时候要条用Unpin
  this->Drop();
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

// ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept :guard_(std::move(that.guard_)){
//     // this->guard_(std::move(that.guard_));
//     // this->guard_.bpm_=nullptr;
// };

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  // this->guard_.bpm_ = that.guard_.bpm_;
  // that.guard_.bpm_ = nullptr;
  // this->guard_.page_ = that.guard_.page_;
  // that.guard_.page_ = nullptr;
  // this->guard_.is_dirty_ = that.guard_.is_dirty_;
  // that.guard_.is_dirty_ = false;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_.Drop();
  // 这时候就可以使用BasicPageGuard的移动赋值函数了
  this->guard_ = std::move(that.guard_);
  // that.guard_.bpm_ = nullptr;
  // that.guard_.page_ = nullptr;
  // that.guard_.is_dirty_ = false;
  return *this;
}

void ReadPageGuard::Drop() {
  // if(this->guard_.bpm_ != nullptr && this->guard_.page_ != nullptr){
  //     this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(),this->guard_.is_dirty_);
  // }
  // this->guard_.bpm_ = nullptr;
  // this->guard_.page_= nullptr;
  // this->guard_.is_dirty_ = false;
  // this->guard_.page_->RLatch();
  if (this->guard_.page_ != nullptr) this->guard_.page_->RUnlatch();
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_.Drop();
  // 这时候就可以使用BasicPageGuard的移动赋值函数了
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) this->guard_.page_->WUnlatch();
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
