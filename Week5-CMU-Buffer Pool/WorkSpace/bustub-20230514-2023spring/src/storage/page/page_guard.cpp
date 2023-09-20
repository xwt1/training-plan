#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    // this->bpm_ = that.bpm_;
    that.bpm_ = nullptr;
    // this->page_ = that.page_;
    that.page_ = nullptr;
    // this->is_dirty_ = that.is_dirty_;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    // auto drop_frame_id = this->bpm_->page_table_[this->page->page_id_];
    if(this->bpm_ != nullptr && this->page_ != nullptr){
        this->bpm_->UnpinPage(this->page_->GetPageId(),this->is_dirty_);
    }
    this->bpm_ = nullptr;
    this->page_= nullptr;
    this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
    //这里我直接drop原先的页面,感觉不太对,他的注释光说要考虑有啥动作需要考虑，
    //当原先的页面被覆盖的时候。
    //但是目前动作貌似就drop一个，只好drop了,就很奇怪为啥这时候要条用Unpin
    this->Drop();
    // this->bpm_ = that.bpm_;
    that.bpm_ = nullptr;
    // this->page_ = that.page_;
    that.page_ = nullptr;
    // this->is_dirty_ = that.is_dirty_;
    that.is_dirty_ = false;
    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    this->Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default{
    this->guard_(std::move(that.guard_));
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    
    return *this; 
}

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
