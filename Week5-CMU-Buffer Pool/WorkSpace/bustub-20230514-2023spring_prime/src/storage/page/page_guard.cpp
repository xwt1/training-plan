#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
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
  if (this == &that) {
    return *this;
  }
  this->Drop();
  this->bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  this->page_ = that.page_;
  that.page_ = nullptr;
  this->is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  if (guard_.page_ != nullptr) {
    // 如果之前存在page,则必须将它的锁打开，否则move的右值给覆盖之后，原来的那个page就死锁了
    guard_.page_->RUnlatch();
  }

  // 这时候就可以使用BasicPageGuard的移动赋值函数了
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  if (this->guard_.page_ != nullptr) {
    // 如果之前存在page,则必须将它的锁打开，否则move的右值给覆盖之后，原来的那个page就死锁了
    this->guard_.page_->WUnlatch();
  }
  // 这时候就可以使用BasicPageGuard的移动赋值函数了
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
  }
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
