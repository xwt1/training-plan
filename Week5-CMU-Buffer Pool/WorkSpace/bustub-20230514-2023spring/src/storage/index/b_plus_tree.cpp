#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;

  // std::cout << leaf_max_size << " " << internal_max_size << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 两个写法一致，为什么？
  // auto guard = this->bpm_->FetchPageRead(this->header_page_id_);
  // auto root_page = guard.template As<BPlusTreeHeaderPage>();
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  // WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
  // return true;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  if (!this->IsEmpty()) {
    ReadPageGuard header_guard = this->bpm_->FetchPageRead(this->header_page_id_);
    auto header_page = header_guard.As<BPlusTreeHeaderPage>();
    ReadPageGuard root_page_guard = this->bpm_->FetchPageRead(header_page->root_page_id_);
    header_guard.Drop();
    Context ctx;
    ctx.read_set_.push_back(std::move(root_page_guard));
    // 自此,拿到了root page(结点),将其存储在read_set_中
    bool ret_flag = false;
    auto leaf_cmp_func = [&](const MappingType &a, const MappingType &b) -> bool {
      return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
    };
    auto internal_cmp_func = [&](const std::pair<KeyType, page_id_t> &a,
                                 const std::pair<KeyType, page_id_t> &b) -> bool {
      return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
    };

    while (!ctx.read_set_.empty()) {
      auto now_page = ctx.read_set_.back().As<BPlusTreePage>();
      if (now_page->IsLeafPage()) {
        auto now_leaf_page = reinterpret_cast<const LeafPage *>(now_page);
        // 这时候找一下有没有关键字就可以了
        // 这个地方不知道leafpage的数组下标是不是从0开始的。
        auto now_leaf_page_array = now_leaf_page->GetArray();
        auto key_itr = std::lower_bound(now_leaf_page_array, now_leaf_page_array + now_leaf_page->GetSize(),
                                        std::make_pair(key, now_leaf_page_array[0].second), leaf_cmp_func);
        int key_index = key_itr - now_leaf_page_array;
        if (key_index < now_leaf_page->GetSize()) {
          if (this->comparator_(now_leaf_page_array[key_index].first, key) == 0) {
            ret_flag = true;
            auto now_rid = now_leaf_page_array[key_index].second;
            result->push_back(now_rid);
          }
        }
        // 把锁全部释放掉
        header_guard.Drop();
        while (!ctx.read_set_.empty()) {
          ctx.read_set_.pop_front();
        }
        continue;
      }
      auto now_internal_page = reinterpret_cast<const InternalPage *>(now_page);
      auto now_internal_page_array = now_internal_page->GetArray();
      // 这时候需要接着寻找,压入read_set_
      auto key_pos =
          std::upper_bound(now_internal_page_array + 1, now_internal_page_array + now_internal_page->GetSize(),
                           std::make_pair(key, now_internal_page_array[0].second), internal_cmp_func) -
          1;
      int key_index = key_pos - now_internal_page_array;
      ReadPageGuard son_page_guard = this->bpm_->FetchPageRead(now_internal_page_array[key_index].second);
      ctx.read_set_.push_back(std::move(son_page_guard));
      ctx.read_set_.pop_front();
    }
    return ret_flag;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  WritePageGuard header_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(header_guard));
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // 若为空,则新建一个page
    page_id_t new_page_id = -233;
    this->BuildNewPage(&new_page_id, IndexPageType::LEAF_PAGE);
    header_page->root_page_id_ = new_page_id;
  }
  WritePageGuard root_page_guard = this->bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(std::move(root_page_guard));
  // 开始查找可以插入的点,并且当插入违反B_PLUS_TREE规则时分裂结点
  bool ret_flag = false;
  // reach_leaf用来判断是否到达过leaf点,last_split存储栈信息中如果上一次是否有要添加的值
  // 如果在返回的过程中,有需要加入当前结点的key值,则将上一次循环的新生成的两个叶子page的page_id存储起来
  bool reach_leaf = false;
  std::optional<KeyType> last_split = std::nullopt;
  std::optional<std::pair<page_id_t, page_id_t>> left_right_id = std::nullopt;
  while (!ctx.write_set_.empty()) {
    auto now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    auto now_page_id = ctx.write_set_.back().PageId();
    auto cmp_func = [&](const MappingType &a, const MappingType &b) -> bool {
      return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
    };
    if (now_page->IsLeafPage()) {
      reach_leaf = true;
      auto now_leaf_page = reinterpret_cast<LeafPage *>(now_page);
      auto now_leaf_page_array = now_leaf_page->GetArray();
      auto lower = std::lower_bound(now_leaf_page_array, now_leaf_page_array + now_leaf_page->GetSize(),
                                    std::make_pair(key, value), cmp_func);
      if (lower - now_leaf_page_array < now_leaf_page->GetSize()) {
        // 保证找到的位置是有效的
        if (this->comparator_(lower->first, key) == 0) {
          // 说明有重复的键值,返回false;
          ret_flag = false;
          (*ctx.header_page_).Drop();
          while (!ctx.write_set_.empty()) {
            ctx.write_set_.pop_front();
          }
          break;
        }
      }
      ret_flag = true;
      int right_node_index_num = lower - now_leaf_page_array;
      // 此时应该先插入,再看一下有没有超过上界
      this->InsertLeafAValue(now_leaf_page, std::make_pair(key, value), right_node_index_num);
      if (now_leaf_page->GetSize() == now_leaf_page->GetMaxSize()) {
        // 由于达到上限,此时应该要分裂
        page_id_t new_right_page_id = -233;
        this->BuildNewPage(&new_right_page_id, IndexPageType::LEAF_PAGE);
        WritePageGuard new_right_page_guard = this->bpm_->FetchPageWrite(new_right_page_id);
        auto new_right_page = new_right_page_guard.AsMut<LeafPage>();

        // 将结点右半部分移到新结点中,并将原先的结点Size调整为原先的一半。
        int half_index = now_leaf_page->GetSize() / 2;

        // 将右半部分移动到新的右子树
        now_leaf_page_array = now_leaf_page->GetArray();  // 重新获取一次插入后的数组
        std::vector<MappingType> move_to_right;
        for (int i = half_index; i < now_leaf_page->GetSize(); i++) {
          move_to_right.push_back(now_leaf_page_array[i]);
        }
        now_leaf_page->SetSize(half_index);
        int siz = move_to_right.size();
        new_right_page->SetSize(siz);
        for (int i = 0; i < siz; i++) {
          new_right_page->SetArrayAt(i, move_to_right[i]);
        }
        // 存储需要传给上层的MappingType以及新的左右孩子的page_id
        last_split = std::make_optional(now_leaf_page->KeyAt(half_index));
        left_right_id = std::make_optional(std::make_pair(now_page_id, new_right_page_id));
        new_right_page->SetNextPageId(now_leaf_page->GetNextPageId());
        now_leaf_page->SetNextPageId(new_right_page_id);
        // 这里要判断一下是不是根节点，如果是根节点需要更新header
        if (ctx.IsRootPage(now_page_id)) {
          page_id_t new_root_id = -233;
          this->BuildNewPage(&new_root_id, IndexPageType::INTERNAL_PAGE);
          WritePageGuard new_root_page_guard = this->bpm_->FetchPageWrite(new_root_id);
          auto new_root_page = new_root_page_guard.AsMut<InternalPage>();
          new_root_page->SetKeyAt(1, *last_split);
          // 左子树肯定是原root本身,右子树是新建的结点
          new_root_page->SetValueAt(0, (*left_right_id).first);
          new_root_page->SetValueAt(1, (*left_right_id).second);
          new_root_page->SetSize(2);
          // 更新header
          header_page->root_page_id_ = new_root_id;
        }
      } else {
        last_split = std::nullopt;
        left_right_id = std::nullopt;
      }
      // 如果此时已经没有需要上传的,就把写锁全部释放掉
      if (last_split == std::nullopt && left_right_id == std::nullopt) {
        (*ctx.header_page_).Drop();
        while (!ctx.write_set_.empty()) {
          ctx.write_set_.pop_front();
        }
        continue;
      }
      // 把leaf_page弹出去,这个地方有点不太清楚,bustub提示我说要"pop from the write_set_ and drop",
      // 但是我想pop_back完了之后,在析构的时候应该会自动调用drop的,应该不用手动drop?这里尝试把手动drop的注释掉看看对不对
      ctx.write_set_.pop_back();
    } else {
      auto internal_cmp_func = [&](const std::pair<KeyType, page_id_t> &a,
                                   const std::pair<KeyType, page_id_t> &b) -> bool {
        return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
      };
      // 说明此时是Internal page
      auto now_internal_page = reinterpret_cast<InternalPage *>(now_page);
      auto now_internal_page_array = now_internal_page->GetArray();
      if (reach_leaf) {
        // 此时已经到达过叶子结点,故需要处理有没有传上来的需要插入的结点
        if (last_split != std::nullopt && left_right_id != std::nullopt) {
          // 这个地方我选择重新二分查找要插入last_split的index,但是实际上应该可以自己写个栈保留一下当前这个internalPage
          // 找到的孩子结点的键值对的索引,可以减少一个对数复杂度,但是我懒得写.如果因此TLE寄了就寄了吧,工作量过大
          auto key_pos =
              std::upper_bound(now_internal_page_array + 1, now_internal_page_array + now_internal_page->GetSize(),
                               std::make_pair((*last_split), (*left_right_id).second), internal_cmp_func);
          int key_index = key_pos - now_internal_page_array;
          if (now_internal_page->GetSize() == now_internal_page->GetMaxSize()) {
            // 在插入前Size就已经是Max了,则必须分裂
            KeyType key_to_push;
            std::pair<page_id_t, page_id_t> left_right_son_to_push;
            this->DealWithInternalSplit(now_internal_page, key_index, (*last_split), (*left_right_id), key_to_push,
                                        left_right_son_to_push);
            left_right_son_to_push.first = now_page_id;
            // 存储需要传给上层的MappingType以及新的左右孩子的page_id
            last_split = std::make_optional(key_to_push);
            left_right_id = std::make_optional(left_right_son_to_push);
            if (ctx.IsRootPage(now_page_id)) {
              page_id_t new_root_id = -233;
              this->BuildNewPage(&new_root_id, IndexPageType::INTERNAL_PAGE);
              WritePageGuard new_root_page_guard = this->bpm_->FetchPageWrite(new_root_id);
              auto new_root_page = new_root_page_guard.AsMut<InternalPage>();
              new_root_page->SetKeyAt(1, *last_split);
              // 左子树肯定是原root本身
              new_root_page->SetValueAt(0, (*left_right_id).first);
              new_root_page->SetValueAt(1, (*left_right_id).second);
              new_root_page->SetSize(2);
              // 更新header
              header_page->root_page_id_ = new_root_id;
              last_split = std::nullopt;
              left_right_id = std::nullopt;
            }
          } else {
            // 否则直接插入
            this->InsertInternalAValue(now_internal_page, std::make_pair(*last_split, (*left_right_id).second),
                                       key_index);
            last_split = std::nullopt;
            left_right_id = std::nullopt;
          }
        } else {
          // 如果此时已经没有需要上传的,就把写锁全部释放掉
          if (last_split == std::nullopt && left_right_id == std::nullopt) {
            (*ctx.header_page_).Drop();
            while (!ctx.write_set_.empty()) {
              ctx.write_set_.pop_front();
            }
            continue;
          }
        }
        ctx.write_set_.pop_back();
      } else {
        // 注意这里需要从1号位置开始二分,因为1号不符合要求,key_pos对应的位置就是
        auto key_pos =
            std::upper_bound(now_internal_page_array + 1, now_internal_page_array + now_internal_page->GetSize(),
                             std::make_pair(key, -233), internal_cmp_func) -
            1;
        int key_index = key_pos - now_internal_page_array;
        auto son_page_id = now_internal_page_array[key_index].second;
        WritePageGuard son_page_guard = this->bpm_->FetchPageWrite(son_page_id);
        ctx.write_set_.push_back(std::move(son_page_guard));
      }
    }
  }
  (*ctx.header_page_).Drop();
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_front();
  }
  return ret_flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  WritePageGuard header_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(header_guard));
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // 立即返回
    return;
  }
  WritePageGuard root_page_guard = this->bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(std::move(root_page_guard));
  // reach_leaf用来判断是否到达过leaf点,last_split存储栈信息中如果上一次是否有要添加的值
  // 如果在返回的过程中,有需要加入当前结点的key值,则将上一次循环的新生成的两个叶子page的page_id存储起来
  bool reach_leaf = false;
  while (!ctx.write_set_.empty()) {
    auto now_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    auto now_page_id = ctx.write_set_.back().PageId();
    auto cmp_func = [&](const MappingType &a, const MappingType &b) -> bool {
      return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
    };
    auto internal_cmp_func = [&](const std::pair<KeyType, page_id_t> &a,
                                 const std::pair<KeyType, page_id_t> &b) -> bool {
      return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
    };
    if (now_page->IsLeafPage()) {
      // 如果是叶子结点
      reach_leaf = true;
      auto now_leaf_page = reinterpret_cast<LeafPage *>(now_page);
      auto now_leaf_page_array = now_leaf_page->GetArray();
      // 这里的now_leaf_page_array[0].first没有意义
      auto delete_itr = std::lower_bound(now_leaf_page_array, now_leaf_page_array + now_leaf_page->GetSize(),
                                         std::make_pair(key, now_leaf_page_array[0].second), cmp_func);
      int delete_index = delete_itr - now_leaf_page_array;
      if (delete_index >= now_leaf_page->GetSize()) {
        // 判定delete_index超过了上限,直接退出循环
        (*ctx.header_page_).Drop();
        while (!ctx.write_set_.empty()) {
          ctx.write_set_.pop_front();
        }
        continue;
      }
      if (this->comparator_(key, delete_itr->first) == -1) {
        // 说明找到的值比key大,直接退出循环
        (*ctx.header_page_).Drop();
        while (!ctx.write_set_.empty()) {
          ctx.write_set_.pop_front();
        }
        continue;
      }
      // 否则说明找到了要删除的值在delete_index
      // 叶子结点先删完,再判断是否小于ceil(MaxSize / 2)
      bool is_coalesce = false;
      if (now_page_id == ctx.root_page_id_) {
        // 说明要删除值的叶子结点是根节点,特殊处理一下
        now_leaf_page->DeleteAValue(delete_index);
        if (now_leaf_page->GetSize() <= 0) {
          // 说明这时候为空,需要将header重新配置
          // 同时考虑要不要调用this->bpm_->DeletePage()?
          this->bpm_->DeletePage(now_page_id);
          header_page->root_page_id_ = INVALID_PAGE_ID;
        }
      } else {
        // 不是根节点,正常删除值
        now_leaf_page->DeleteAValue(delete_index);
        if (now_leaf_page->GetSize() < now_leaf_page->GetMinSize()) {
          // 小于最小值,需要借值合并操作
          is_coalesce = this->BorrowOrCoalesceLeafPage(now_leaf_page, now_page_id, ctx.parent_.top());
        } else {
          // 否则直接删除就可以了
        }
      }
      if (!is_coalesce) {
        // 这时候说明没有合并操作使得父亲结点发生变化，可以提前把锁放掉了
        (*ctx.header_page_).Drop();
        while (!ctx.write_set_.empty()) {
          ctx.write_set_.pop_front();
        }
        continue;
      }
      ctx.write_set_.pop_back();
      ctx.parent_.pop();
    } else {
      auto now_internal_page = reinterpret_cast<InternalPage *>(now_page);
      auto now_internal_page_array = now_internal_page->GetArray();
      if (reach_leaf) {
        // 说明到达过叶子结点
        bool is_coalesce = false;
        if (now_page_id == ctx.root_page_id_) {
          if (now_internal_page->GetSize() <= 1) {
            // 如果根节点作为内部结点,键值对的数量小于等于1,特殊处理它的叶子结点为根节点
            header_page->root_page_id_ = now_internal_page->ValueAt(0);
            // 并且考虑这个地方考虑一下调不调用this->bpm_->DeletePage()?
            this->bpm_->DeletePage(now_page_id);
          }
        } else {
          // 否则这个地方首先看一下需不需要合并或者借值操作
          if (now_internal_page->GetSize() < now_internal_page->GetMinSize()) {
            // 说明需要合并或者借值操作
            is_coalesce = this->BorrowOrCoalesceInternalPage(now_internal_page, now_page_id, ctx.parent_.top());
          }
        }
        ctx.write_set_.pop_back();
        ctx.parent_.pop();
        if (is_coalesce) {
          // 说明有合并操作
          // 这个地方考虑一下调不调用this->bpm_->DeletePage()?
          this->bpm_->DeletePage(now_page_id);
        } else {
          // 这时候说明没有合并操作使得父亲结点发生变化，可以提前把锁放掉了
          (*ctx.header_page_).Drop();
          while (!ctx.write_set_.empty()) {
            ctx.write_set_.pop_front();
          }
          // header_guard.Drop();
          continue;
        }
      } else {
        // 说明还未到达过叶子结点
        auto delete_itr =
            std::upper_bound(now_internal_page_array + 1, now_internal_page_array + now_internal_page->GetSize(),
                             std::make_pair(key, now_internal_page_array[0].second), internal_cmp_func) -
            1;

        int key_index = delete_itr - now_internal_page_array;
        auto son_page_id = now_internal_page_array[key_index].second;
        WritePageGuard son_page_guard = this->bpm_->FetchPageWrite(son_page_id);
        ctx.write_set_.push_back(std::move(son_page_guard));

        // 记得放入当前结点进parent
        ctx.parent_.push(std::make_pair(now_page, key_index));
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowOrCoalesceLeafPage(LeafPage *page, page_id_t page_id, std::pair<BPlusTreePage *, int> parent)
    -> bool {
  if (parent.first->IsLeafPage()) {
    throw("有个父亲节点是叶子结点");
  }
  auto parent_internal_page = reinterpret_cast<InternalPage *>(parent.first);
  // 添加屎山代码中
  // 将可以给予键值对的兄弟结点装入
  bool is_left_sibing_ok = false;
  std::vector<WritePageGuard> sibing_that_can_give_key;
  if (parent.second > 0) {
    auto left_page_id = parent_internal_page->ValueAt(parent.second - 1);
    WritePageGuard left_page_guard = this->bpm_->FetchPageWrite(left_page_id);
    auto left_page = left_page_guard.AsMut<LeafPage>();
    // 把左兄弟读出来判断一下兄弟能不能借个键值对
    if (left_page->GetSize() > left_page->GetMinSize()) {
      sibing_that_can_give_key.push_back(std::move(left_page_guard));
      is_left_sibing_ok = true;
    }
  }
  // 右兄弟:
  if (parent.second < parent_internal_page->GetSize() - 1 && !is_left_sibing_ok) {
    auto right_page_id = parent_internal_page->ValueAt(parent.second + 1);
    WritePageGuard right_page_guard = this->bpm_->FetchPageWrite(right_page_id);
    auto right_page = right_page_guard.AsMut<LeafPage>();
    if (right_page->GetSize() > right_page->GetMinSize()) {
      sibing_that_can_give_key.push_back(std::move(right_page_guard));
    }
  }
  if (sibing_that_can_give_key.empty()) {
    // 说明这时候肯定要合并
    // 理论上来说,合并到右兄弟是可行的,但是,由于CMU没有提供双向NextPageID,
    // 导致不行,至少我试了一些办法,全是超时,浪费宝贵时间，SB bustub结构
    bool is_left_sibing_coalesce = false;
    if ((parent.second - 1) >= 0) {
      is_left_sibing_coalesce = true;
    }
    if (is_left_sibing_coalesce) {
      // 如果左兄弟存在,就让page合并到左兄弟
      auto left_right_page_id = parent_internal_page->ValueAt(parent.second - 1);
      WritePageGuard left__right_page_guard = this->bpm_->FetchPageWrite(left_right_page_id);
      auto left_right_page = left__right_page_guard.AsMut<LeafPage>();
      int siz = page->GetSize();
      left_right_page->SetNextPageId(page->GetNextPageId());
      for (int i = 0; i < siz; i++) {
        this->InsertLeafAValue(left_right_page, std::make_pair(page->KeyAt(i), page->ValueAt(i)),
                               left_right_page->GetSize());
      }
      parent_internal_page->DeleteAValue(parent.second);
      this->bpm_->DeletePage(page_id);
    } else {
      // 否则就让右兄弟合并到page
      auto left_right_page_id = parent_internal_page->ValueAt(parent.second + 1);
      WritePageGuard left__right_page_guard = this->bpm_->FetchPageWrite(left_right_page_id);
      auto left_right_page = left__right_page_guard.AsMut<LeafPage>();
      int siz = left_right_page->GetSize();
      page->SetNextPageId(left_right_page->GetNextPageId());
      for (int i = 0; i < siz; i++) {
        this->InsertLeafAValue(page, std::make_pair(left_right_page->KeyAt(i), left_right_page->ValueAt(i)),
                               page->GetSize());
      }
      parent_internal_page->DeleteAValue(parent.second + 1);
      this->bpm_->DeletePage(left_right_page_id);
    }
    return true;
  }
  // 说明这时候可以借一个数
  auto left_right_page = sibing_that_can_give_key[0].AsMut<LeafPage>();
  if (is_left_sibing_ok) {
    // 首先覆盖父亲原先分界点的值:
    parent_internal_page->SetKeyAt(parent.second, left_right_page->KeyAt(left_right_page->GetSize() - 1));
    // 接下来将左兄弟的最后一个值替代父亲原先的键值,并且将key-value插入到原结点中
    this->InsertLeafAValue(page,
                           std::make_pair(left_right_page->KeyAt(left_right_page->GetSize() - 1),
                                          left_right_page->ValueAt(left_right_page->GetSize() - 1)),
                           0);
    left_right_page->DeleteAValue(left_right_page->GetSize() - 1);
  } else {
    this->InsertLeafAValue(page, std::make_pair(left_right_page->KeyAt(0), left_right_page->ValueAt(0)),
                           page->GetSize());

    left_right_page->DeleteAValue(0);

    parent_internal_page->SetKeyAt(parent.second + 1, left_right_page->KeyAt(0));
  }
  // 最后记得让左或右兄弟的大小减一
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowOrCoalesceInternalPage(InternalPage *page, page_id_t page_id,
                                                  std::pair<BPlusTreePage *, int> parent) -> bool {
  if (parent.first->IsLeafPage()) {
    throw("有个父亲节点是叶子结点");
  }
  auto parent_internal_page = reinterpret_cast<InternalPage *>(parent.first);
  // 添加屎山代码中
  // 将可以给予键值对的兄弟结点装入
  bool is_left_sibing_ok = false;
  std::vector<WritePageGuard> sibing_that_can_give_key;
  if (parent.second > 0) {
    auto left_page_id = parent_internal_page->ValueAt(parent.second - 1);
    WritePageGuard left_page_guard = this->bpm_->FetchPageWrite(left_page_id);
    auto left_page = left_page_guard.AsMut<InternalPage>();
    // 把左兄弟读出来判断一下兄弟能不能借个键值对
    if (left_page->GetSize() > left_page->GetMinSize()) {
      sibing_that_can_give_key.push_back(std::move(left_page_guard));
      is_left_sibing_ok = true;
    }
  }
  // 右兄弟:
  if (parent.second < parent_internal_page->GetSize() - 1 && !is_left_sibing_ok) {
    auto right_page_id = parent_internal_page->ValueAt(parent.second + 1);
    WritePageGuard right_page_guard = this->bpm_->FetchPageWrite(right_page_id);
    auto right_page = right_page_guard.AsMut<InternalPage>();
    if (right_page->GetSize() > right_page->GetMinSize()) {
      sibing_that_can_give_key.push_back(std::move(right_page_guard));
    }
  }
  if (sibing_that_can_give_key.empty()) {
    // 说明这时候肯定要合并
    bool is_left_sibing_coalesce = false;
    if ((parent.second - 1) >= 0) {
      is_left_sibing_coalesce = true;
    }
    auto left_right_page_id =
        parent_internal_page->ValueAt(is_left_sibing_coalesce ? parent.second - 1 : parent.second + 1);
    WritePageGuard left__right_page_guard = this->bpm_->FetchPageWrite(left_right_page_id);
    auto left_right_page = left__right_page_guard.AsMut<InternalPage>();
    int siz = page->GetSize();
    if (is_left_sibing_coalesce) {
      // 合并到左子树
      for (int i = 0; i < siz; i++) {
        // int pos = i;
        if (i == 0) {
          this->InsertInternalAValue(left_right_page,
                                     std::make_pair(parent_internal_page->KeyAt(parent.second), page->ValueAt(i)),
                                     left_right_page->GetSize());
        } else {
          this->InsertInternalAValue(left_right_page, std::make_pair(page->KeyAt(i), page->ValueAt(i)),
                                     left_right_page->GetSize());
        }
      }
    } else {
      // 合并到右子树
      left_right_page->SetKeyAt(0, parent_internal_page->KeyAt(parent.second + 1));
      for (int i = siz - 1; i >= 0; i--) {
        this->InsertInternalAValue(left_right_page, std::make_pair(page->KeyAt(i), page->ValueAt(i)), 0);
      }
    }
    // 将父亲结点对应位置的值删除
    // 这里需不需要调用this->bpm_->DeletePage()清除孩子结点占用的page?不过这里应该清除不了,得去外面清除需要考虑一下
    parent_internal_page->DeleteAValue(parent.second);
    return true;
  }
  // 说明这时候可以借一个数
  auto left_right_page = sibing_that_can_give_key[0].AsMut<InternalPage>();
  if (is_left_sibing_ok) {
    // 首先将父亲的Key值传给page的首元素
    page->SetKeyAt(0, parent_internal_page->KeyAt(parent.second));
    // 再将左兄弟的最后一个Key值覆盖到父亲结点
    parent_internal_page->SetKeyAt(parent.second, left_right_page->KeyAt(left_right_page->GetSize() - 1));
    // 最后再插入
    this->InsertInternalAValue(page,
                               std::make_pair(left_right_page->KeyAt(left_right_page->GetSize() - 1),
                                              left_right_page->ValueAt(left_right_page->GetSize() - 1)),
                               0);

    left_right_page->DeleteAValue(left_right_page->GetSize() - 1);
  } else {
    this->InsertInternalAValue(
        page, std::make_pair(parent_internal_page->KeyAt(parent.second + 1), left_right_page->ValueAt(0)),
        page->GetSize());

    left_right_page->DeleteAValue(0);

    parent_internal_page->SetKeyAt(parent.second + 1, left_right_page->KeyAt(0));
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard root_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = root_page_guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // 根节点为空,直接返回空
    return End();
  }
  page_id_t page_id = root_page->root_page_id_;
  ReadPageGuard now_page_guard = bpm_->FetchPageRead(page_id);
  root_page_guard.Drop();
  auto *now_page = now_page_guard.As<BPlusTreePage>();
  while (!now_page->IsLeafPage()) {
    auto *now_internal_page = reinterpret_cast<const InternalPage *>(now_page);
    page_id = now_internal_page->ValueAt(0);
    now_page_guard = bpm_->FetchPageRead(page_id);
    now_page = now_page_guard.As<BPlusTreePage>();
  }
  // return INDEXITERATOR_TYPE(bpm_, std::move(now_page_guard), std::move(root_page_guard), 0);
  return INDEXITERATOR_TYPE(bpm_, std::move(now_page_guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto cmp_func = [&](const MappingType &a, const MappingType &b) -> bool {
    return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
  };
  auto internal_cmp_func = [&](const std::pair<KeyType, page_id_t> &a, const std::pair<KeyType, page_id_t> &b) -> bool {
    return static_cast<bool>(this->comparator_(a.first, b.first) == -1);
  };
  ReadPageGuard root_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = root_guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    // Page is empty, simply return empty
    return End();
  }
  page_id_t page_id = root_page->root_page_id_;
  ReadPageGuard now_page_guard = bpm_->FetchPageRead(page_id);
  root_guard.Drop();
  auto now_page = now_page_guard.As<BPlusTreePage>();
  while (!now_page->IsLeafPage()) {
    auto now_internal_page = now_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    auto now_internal_page_array = now_internal_page->GetArray();
    auto key_pos = std::upper_bound(now_internal_page_array + 1, now_internal_page_array + now_internal_page->GetSize(),
                                    std::make_pair(key, now_internal_page_array[0].second), internal_cmp_func) -
                   1;
    int key_index = key_pos - now_internal_page_array;
    page_id = now_internal_page->ValueAt(key_index);
    now_page_guard = bpm_->FetchPageRead(page_id);
    now_page = now_page_guard.As<BPlusTreePage>();
  }

  auto now_leaf_page = now_page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  auto now_leaf_page_array = now_leaf_page->GetArray();
  auto key_itr = std::lower_bound(now_leaf_page_array, now_leaf_page_array + now_leaf_page->GetSize(),
                                  std::make_pair(key, now_leaf_page_array[0].second), cmp_func);
  int key_index = key_itr - now_leaf_page_array;
  if (key_index >= now_leaf_page->GetSize()) {
    return End();
  }
  if (this->comparator_(now_leaf_page_array[key_index].first, key) == 0) {
    // return INDEXITERATOR_TYPE(bpm_, std::move(now_page_guard), std::move(root_guard), key_index);
    return INDEXITERATOR_TYPE(bpm_, std::move(now_page_guard), key_index);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // return INDEXITERATOR_TYPE(nullptr, {nullptr, nullptr}, {nullptr, nullptr}, -233);
  return INDEXITERATOR_TYPE(nullptr, {nullptr, nullptr}, -233);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  WritePageGuard header_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeafAValue(LeafPage *pages, const MappingType &value, int pos) {
  std::vector<MappingType> move_to_right;
  auto page_array = pages->GetArray();
  for (int i = pos; i < pages->GetSize(); i++) {
    move_to_right.push_back(page_array[i]);
  }
  pages->SetArrayAt(pos, std::move(std::make_pair(value.first, value.second)));
  pages->IncreaseSize(1);
  int new_start_index = pos + 1;
  for (int i = new_start_index; i < pages->GetSize(); i++) {
    pages->SetArrayAt(i, std::move(move_to_right[i - new_start_index]));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalAValue(InternalPage *pages, const std::pair<KeyType, page_id_t> &value, int pos) {
  std::vector<std::pair<KeyType, page_id_t>> move_to_right;
  // std::vector <MappingType> move_to_right;
  auto page_array = pages->GetArray();
  for (int i = pos; i < pages->GetSize(); i++) {
    move_to_right.push_back(page_array[i]);
  }
  pages->SetArrayAt(pos, std::move(std::make_pair(value.first, value.second)));
  pages->IncreaseSize(1);
  int new_start_index = pos + 1;
  for (int i = new_start_index; i < pages->GetSize(); i++) {
    pages->SetArrayAt(i, std::move(move_to_right[i - new_start_index]));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DealWithInternalSplit(InternalPage *pages, int key_index, const KeyType &key,
                                           const std::pair<page_id_t, page_id_t> &left_right_son, KeyType &key_to_push,
                                           std::pair<page_id_t, page_id_t> &left_right_son_to_push) {
  std::vector<std::pair<KeyType, page_id_t>> prime_internal_page;
  auto pages_array = pages->GetArray();
  page_id_t new_right_page_id = -233;
  this->BuildNewPage(&new_right_page_id, IndexPageType::INTERNAL_PAGE);
  WritePageGuard new_right_page_guard = this->bpm_->FetchPageWrite(new_right_page_id);
  auto new_right_page = new_right_page_guard.AsMut<InternalPage>();
  for (int i = 0; i < pages->GetSize(); i++) {
    if (i == key_index) {
      prime_internal_page.push_back(std::make_pair(key, left_right_son.second));
    }
    prime_internal_page.push_back(pages_array[i]);
  }
  if (key_index == pages->GetSize()) {
    // 这种情况要插入到最后面
    prime_internal_page.push_back(std::make_pair(key, left_right_son.second));
  }
  int siz = prime_internal_page.size();
  key_to_push = prime_internal_page[siz / 2].first;
  pages->SetSize(siz / 2);
  for (int i = 0; i < pages->GetSize(); i++) {
    pages->SetArrayAt(i, prime_internal_page[i]);
  }
  int new_start_index = siz / 2;
  // 这里new_right_page的第一个key应该要设个非法值
  // new_right_page->SetKeyAt(0,0);  //但是不知道怎么设置
  new_right_page->SetValueAt(0, prime_internal_page[new_start_index].second);
  for (int i = new_start_index + 1; i < siz; i++) {
    new_right_page->SetArrayAt(i - new_start_index, prime_internal_page[i]);
  }
  new_right_page->SetSize(siz - pages->GetSize());
  // left_right_son_to_push.first =  //这个地方还拿不到,只有在函数外面拿,我写的锅
  left_right_son_to_push.second = new_right_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DealWithLeafSplit(LeafPage *pages, int key_index, const KeyType &key, const ValueType &value,
                                       KeyType &key_to_push, std::pair<page_id_t, page_id_t> &left_right_son_to_push) {
  std::vector<MappingType> prime_internal_page;
  auto pages_array = pages->GetArray();
  page_id_t new_right_page_id = -233;
  this->BuildNewPage(&new_right_page_id, IndexPageType::LEAF_PAGE);
  WritePageGuard new_right_page_guard = this->bpm_->FetchPageWrite(new_right_page_id);
  auto new_right_page = new_right_page_guard.AsMut<LeafPage>();
  for (int i = 0; i < pages->GetSize(); i++) {
    if (i == key_index) {
      prime_internal_page.push_back(std::make_pair(key, value));
    }
    prime_internal_page.push_back(pages_array[i]);
  }
  if (key_index == pages->GetSize()) {
    // 这种情况要插入到最后面
    prime_internal_page.push_back(std::make_pair(key, value));
  }
  int siz = prime_internal_page.size();
  key_to_push = prime_internal_page[siz / 2].first;
  pages->SetSize(siz / 2);
  for (int i = 0; i < pages->GetSize(); i++) {
    pages->SetArrayAt(i, prime_internal_page[i]);
  }
  int new_start_index = siz / 2;

  // new_right_page->SetKeyAt(0,0);  //但是不知道怎么设置
  // new_right_page->SetValueAt(0, prime_internal_page[new_start_index].second);
  for (int i = new_start_index; i < siz; i++) {
    new_right_page->SetArrayAt(i - new_start_index, prime_internal_page[i]);
  }
  new_right_page->SetSize(siz - pages->GetSize());
  // left_right_son_to_push.first =  //这个地方还拿不到,只有在函数外面拿,我写的锅
  left_right_son_to_push.second = new_right_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BuildNewPage(page_id_t *page_id, IndexPageType page_type) {
  BasicPageGuard new_page_guard = this->bpm_->NewPageGuarded(page_id);
  if (page_type == IndexPageType::LEAF_PAGE) {
    auto *new_page = new_page_guard.AsMut<LeafPage>();
    // 初始化这个page
    new_page->Init(this->leaf_max_size_);
  } else {
    if (page_type == IndexPageType::INTERNAL_PAGE) {
      auto *new_page = new_page_guard.AsMut<InternalPage>();
      new_page->Init(this->internal_max_size_);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
