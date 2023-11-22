#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::GetValue(const std::shared_ptr<const TrieNode> &A, std::string_view now_key_left) const
    -> std::shared_ptr<const TrieNodeWithValue<T>> {
  if (A == nullptr) {
    // std::cout<<"在A == nullptr"<<std::endl;
    return nullptr;
  }
  if (now_key_left.empty()) {
    if (A->is_value_node_) {
      // std::cout<<"在A->is_value_node_"<<std::endl;
      std::shared_ptr<TrieNode> cloned_a = A->Clone();
      auto trie_node_with_value = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cloned_a);
      return trie_node_with_value;
    }
    // std::cout<<"在!A->is_value_node_"<<std::endl;
    return nullptr;
  }
  std::shared_ptr<const TrieNode> child_node = nullptr;
  for (auto &son : A->children_) {
    char seita = son.first;
    if (seita == now_key_left[0]) {
      child_node = son.second;
      break;
    }
  }
  if (child_node == nullptr) {
    // std::cout<<"在child_node == nullptr"<<std::endl;
    return nullptr;
  }
  // std::cout<<"在!child_node == nullptr"<<std::endl;
  return Trie::GetValue<T>(child_node, now_key_left.substr(1, now_key_left.size() - 1));

  // if (!now_key_left.empty()) {
  //   std::shared_ptr<const TrieNode> child_node = nullptr;
  //   if (A != nullptr) {
  //     for (auto &son : A->children_) {
  //       char seita = son.first;
  //       if (seita == now_key_left[0]) {
  //         child_node = son.second;
  //         break;
  //       }
  //     }
  //   }
  //   if (child_node == nullptr) {
  //     return nullptr;
  //   }
  //   return Trie::GetValue<T>(child_node, now_key_left.substr(1, now_key_left.size() - 1));
  // } else {
  //   if (A != nullptr) {
  //     if (!A->is_value_node_) {
  //       return nullptr;
  //     }
  //     std::shared_ptr<TrieNode> cloned_a = A->Clone();
  //     auto trie_node_with_value = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cloned_a);

  //     return trie_node_with_value;
  //   }else{
  //     return nullptr;
  //   }
  // }
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // std::function<std::shared_ptr<TrieNode>(std::shared_ptr<const TrieNode>,std::string_view)> GetValue =
  //   [&](std::shared_ptr<const TrieNode> A ,std::string_view now_key_left)  -> std::shared_ptr<TrieNode>{
  //     if(!now_key_left.empty()){
  //     std::shared_ptr<const TrieNode> child_node = nullptr;
  //     for(auto &son:A->children_){
  //       char seita = son.first;
  //       if(seita == now_key_left[0]){
  //         child_node = son.second;
  //         break;
  //       }
  //     }
  //     if(child_node == nullptr){
  //       return nullptr;
  //     }
  //     return GetValue(child_node,now_key_left.substr(1,now_key_left.size()-1));
  //   }else{
  //     if(!A->is_value_node_){
  //       return nullptr;
  //     }
  //     auto trie_node_with_value = std::shared_ptr<TrieNodeWithValue<T>>(std::move(A->Clone()));
  //     // trie_node_with_value
  //     return trie_node_with_value->value_;
  //   }
  // };
  auto value_node_ptr = this->GetValue<T>(this->root_, key);
  if (value_node_ptr == nullptr) {
    // If the key is not in the trie, return nullptr.
    return nullptr;
  }
  if (std::is_same<typename decltype(value_node_ptr->value_)::element_type, T>::value) {
    // Otherwise, return the value.
    return value_node_ptr->value_.get();
  }
  // If the key is in the trie but the type is mismatched, return nullptr.
  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::PutValue(const std::shared_ptr<const TrieNode> &A, std::string_view now_key_left, T value) const
    -> std::shared_ptr<const TrieNode> {
  // 重写，以A是否为nullptr讨论

  if (A == nullptr) {
    if (now_key_left.empty()) {
      // std::cout<<"在now_key_left.empty()"<<std::endl;
      auto new_node_a = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      return new_node_a;
    }
    // std::cout<<"在!now_key_left.empty()"<<std::endl;
    std::shared_ptr<TrieNode> new_node_a = std::make_shared<TrieNode>();
    auto new_child_node = this->PutValue(nullptr, now_key_left.substr(1, now_key_left.size() - 1), std::move(value));
    new_node_a->children_[now_key_left[0]] = new_child_node;
    return new_node_a;
  }
  // 所有父亲节点必须替换
  if (now_key_left.empty()) {
    // std::cout<<"在A != nullptr now_key_left.empty()"<<std::endl;
    std::map<char, std::shared_ptr<const TrieNode>> now_children = A->children_;
    auto new_leef_node = std::make_shared<TrieNodeWithValue<T>>(now_children, std::make_shared<T>(std::move(value)));
    // std::cout<<"我打印出来了"<<std::endl;
    return new_leef_node;
  }
  // std::unique_ptr<TrieNode> uniquePtr = A->Clone();  // 调用A的Clone方法，获得std::unique_ptr<TrieNode>
  // std::shared_ptr<TrieNode> new_node_a = std::shared_ptr<TrieNode>(std::move(uniquePtr));  //
  // 使用std::move将unique_ptr的所有权转移到shared_ptr 甚至由于编译器有RVO(编译器优化),还可以等效成下面两句
  // std::unique_ptr<TrieNode> uniquePtr = A->Clone();  // 调用A的Clone方法，获得std::unique_ptr<TrieNode>
  // std::shared_ptr<TrieNode> new_node_a = std::move(uniquePtr);  //
  // 使用std::move将unique_ptr的所有权转移到shared_ptr 这种写法和上面那两句等效 std::shared_ptr<TrieNode> new_node_a
  // = A->Clone();

  std::shared_ptr<TrieNode> new_node_a = A->Clone();
  std::shared_ptr<const TrieNode> child_node = nullptr;
  for (auto &son : new_node_a->children_) {
    char seita = son.first;
    if (seita == now_key_left[0]) {
      child_node = son.second;
      break;
    }
  }
  std::shared_ptr<const TrieNode> new_child_node = nullptr;
  if (child_node == nullptr) {
    // std::cout<<"在child_node == nullptr"<<std::endl;
    new_child_node = this->PutValue(nullptr, now_key_left.substr(1, now_key_left.size() - 1), std::move(value));
  } else {
    // std::cout<<"在child_node != nullptr"<<std::endl;
    new_child_node = this->PutValue(child_node, now_key_left.substr(1, now_key_left.size() - 1), std::move(value));
    // std::cout<<"我打印出来了"<<std::endl;
  }
  new_node_a->children_[now_key_left[0]] = new_child_node;
  return new_node_a;

  // // 返回当前更改后的点
  // if (!now_key_left.empty()) {
  //   std::shared_ptr<const TrieNode> child_node = nullptr;
  //   if (A != nullptr) {
  //     for (auto &son : A->children_) {
  //       char seita = son.first;
  //       if (seita == now_key_left[0]) {
  //         child_node = son.second;
  //         break;
  //       }
  //     }
  //   }
  //   if (child_node == nullptr) {
  //     // 情况是当前字符串还不为空，没有可用的结点，需要创建一个新的TrieNode结点并且继续递归构造
  //     std::cout<<"在child_node == nullptr"<<std::endl;

  //     auto new_child_node = std::make_shared<TrieNode>();
  //     auto new_node_a_insert_node =
  //         this->PutValue(new_child_node, now_key_left.substr(1, now_key_left.size() - 1), std::move(value));
  //     std::shared_ptr<TrieNode> new_node_a = nullptr;
  //     //这个地方一旦原来的A是TrieNodeWithValue就寄了
  //     if (A != nullptr) {
  //       if(A->is_value_node_){
  //         std::shared_ptr<TrieNode> cloned_a = A->Clone();
  //         new_node_a = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(cloned_a);
  //       }else{
  //         new_node_a = std::make_shared<TrieNode>(A->children_);
  //       }
  //     } else {
  //       new_node_a = std::make_shared<TrieNode>();
  //     }
  //     new_node_a->children_[now_key_left[0]] = new_node_a_insert_node;

  //     // std::cout<<"now_key_left: "<< now_key_left<<std::endl;
  //     // for (auto &son : new_node_a->children_) {
  //     //   std::cout<<son.first<<" ";
  //     // }
  //     // std::cout<<std::endl;

  //     return new_node_a;
  //   } else {
  //     std::cout<<"在child_node != nullptr"<<std::endl;
  //     // 情况是当前字符串还不为空，有可用的结点，顺着这个结点继续构造
  //     // std::shared_ptr<TrieNode> new_child_node = std::make_shared<TrieNode>(child_node->children_);
  //     auto new_child_node =
  //         this->PutValue(child_node, now_key_left.substr(1, now_key_left.size() - 1), std::move(value));
  //     auto new_children = A->children_;
  //     new_children[now_key_left[0]] = new_child_node;
  //     auto new_node_a = std::make_shared<TrieNode>(new_children);

  //     return new_node_a;
  //   }
  // } else {
  //   if (A != nullptr) {
  //     if (A->is_value_node_) {
  //       std::cout<<"在A->is_value_node_"<<std::endl;
  //       // // 如果A是一个TrieNodeWithValue，那么直接使用clone函数
  //       // auto new_leef_node = std::shared_ptr<const TrieNodeWithValue<T>>(std::move(A->Clone()));
  //       // auto cloned_a = A->Clone();
  //       // auto new_leef_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cloned_a);

  //       // 直接拿children
  //       std::map<char, std::shared_ptr<const TrieNode>> now_children = A->children_;
  //           auto new_leef_node =
  //       std::make_shared<TrieNodeWithValue<T>>(now_children, std::make_shared<T>(std::move(value)));

  //       // std::shared_ptr<TrieNode> cloned_a = A->Clone();

  //       // auto new_leef_node = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(cloned_a);

  //       // // 这个地方现在改对了,叙述一下下面模板函数恶心我的解决方法:
  //       // // 首先是std::move()解决MoveBlock,其次是必须用std::make_shared<T>解决unique_ptr
  //       // new_leef_node->value_ = std::make_shared<T>(std::move(value));

  //       return new_leef_node;

  //       // return nullptr;
  //     } else {
  //       // 如果A是一个TrieNode，那么要考虑它是否带有孩子结点，来创建一个新的TrieNodeWithValue结点
  //       if (A->children_.empty()) {
  //         std::cout<<"在A->children_.empty()"<<std::endl;
  //         // 如果A的孩子结点为空，则直接创建叶子结点

  //         auto new_leef_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  //         return new_leef_node;
  //         // return nullptr;
  //       } else {
  //         std::cout<<"在!A->children_.empty()"<<std::endl;
  //         auto new_leef_node =
  //             std::make_shared<TrieNodeWithValue<T>>(A->children_, std::make_shared<T>(std::move(value)));
  //         return new_leef_node;
  //         // return nullptr;
  //       }
  //     }
  //   } else {
  //     std::cout<<"在A==nullptr"<<std::endl;

  //     std::shared_ptr<TrieNodeWithValue<T>> new_node_a =
  //         std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  //     return new_node_a;
  //     // return nullptr;
  //   }
  // }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // std::function<std::shared_ptr<TrieNode>(std::shared_ptr<const TrieNode>,std::string_view)> SetValue =
  // [&](std::shared_ptr<const TrieNode> A,std::string_view now_key_left){
  //   if(!now_key_left.empty()){
  //     std::shared_ptr<const TrieNode> child_node = nullptr;
  //     for(auto &son:A->children_){
  //       char seita = son.first;
  //       if(seita == now_key_left[0]){
  //         child_node = son.second;
  //         break;
  //       }
  //     }
  //     if(child_node == nullptr){
  //       // 情况是当前字符串还不为空，没有可用的结点，需要创建一个新的TrieNode结点并且继续递归构造
  //       std::shared_ptr<TrieNode> new_node_a = new TrieNode(A->children_);
  //       auto new_node_a_insert_node = SetValue(new_node_a,now_key_left[1,now_key_left.size()-1]);
  //       new_node_a->children_[now_key_left[0]]=new_node_a_insert_node;
  //       return new_node_a;
  //     }else{
  //       // 情况是当前字符串还不为空，有可用的结点，顺着这个结点继续构造
  //       SetValue(child_node,now_key_left[1,now_key_left.size()-1]);
  //       // 这个地方动作可能有点问题
  //       return A;
  //     }
  //   }else{
  //     if(A->is_value_node_){
  //       // 如果A是一个TrieNodeWithValue，那么直接使用clone函数
  //       std::shared_ptr<TrieNodeWithValue> new_leef_node = std::make_shared<TrieNodeWithValue>(std::move(A.clone()));
  //       new_leef_node->value_ = std::move(std::make_shared<T>(value));
  //       return new_leef_node;
  //     }else{
  //       // 如果A是一个TrieNode，那么要考虑它是否带有孩子结点，来创建一个新的TrieNodeWithValue结点
  //       if(A->children_.empty()){
  //         // 如果A的孩子结点为空，则直接创建叶子结点
  //         std::shared_ptr<TrieNodeWithValue> new_leef_node = new
  //         TrieNodeWithValue(std::move(std::make_shared<T>(value))); return new_leef_node;
  //       }else{
  //         std::shared_ptr<TrieNodeWithValue> new_leef_node = new
  //         TrieNodeWithValue(A->children_,std::move(std::make_shared<T>(value))); return new_leef_node;
  //       }
  //     }
  //   }
  // };
  // SetValue(this->root_,key);
  // auto old_root = std::make_shared<const TrieNode>(this->root_);

  // std::cout<<value<<std::endl;

  auto new_root = this->PutValue<T>(this->root_, key, std::move(value));
  auto new_tree = Trie(new_root);
  return new_tree;

  // return *this;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::RemoveKey(const std::shared_ptr<const TrieNode> &A, std::string_view now_key_left) const
    -> std::pair<std::shared_ptr<const TrieNode>, bool> {
  if (A == nullptr) {
    // 如果探索到的结点已经为空,不管是否还有剩余字符,都是删除失败,返回原来的结点
    // std::cout<<"在A == nullptr"<<std::endl;
    return {nullptr, false};
  }
  if (!now_key_left.empty()) {
    // 如果还有剩余的匹配串
    std::shared_ptr<const TrieNode> child_node = nullptr;
    for (auto &son : A->children_) {
      char seita = son.first;
      if (seita == now_key_left[0]) {
        child_node = son.second;
        break;
      }
    }
    if (child_node == nullptr) {
      // std::cout<<"在child_node == nullptr"<<std::endl;
      // 如果没有找到正确的孩子结点,则说明没有找到
      // auto ret_pair = this->RemoveKey(child_node, now_key_left.substr(1, now_key_left.size() - 1));
      return {A, false};
    }
    // 如果找到,开始dfs
    auto ret_pair = this->RemoveKey(child_node, now_key_left.substr(1, now_key_left.size() - 1));
    if (ret_pair.second) {
      // 这个地方注意nullptr
      // std::cout<<"在ret_pair.second"<<std::endl;
      // 如果底层找到了删除的点,需要新建结点
      std::shared_ptr<TrieNode> new_node_a = A->Clone();
      if (ret_pair.first != nullptr) {
        // 孩子不为空,更新指向当前版本的新孩子结点
        new_node_a->children_[now_key_left[0]] = ret_pair.first;
      } else {
        // 如果发现传回来的孩子节点已经为空,则直接删除这条线
        new_node_a->children_.erase(now_key_left[0]);
      }
      return {new_node_a, true};
    }
    // std::cout<<"在!ret_pair.second"<<std::endl;
    // 如果底层没有找到删除的点,直接返回原来的结点
    return {A, false};
  }
  if (A->is_value_node_) {
    // 为带值结点,符合移除条件
    if (A->children_.empty()) {
      // std::cout<<"在A->children_.empty()"<<std::endl;
      // 如果此时已经是叶子结点,则新树应该不包含这个结点
      return {nullptr, true};
    }
    // std::cout<<"在!A->children_.empty()"<<std::endl;
    // 如果此时不是叶子结点,则新树应该包含这个结点
    // 设置一个新的结点替换原来的结点,并且重新设置为TrieNode
    std::map<char, std::shared_ptr<const TrieNode>> now_children = A->children_;
    std::shared_ptr<TrieNode> new_node_a = std::make_shared<TrieNode>(now_children);
    return {new_node_a, true};
  }
  // std::cout<<"在!A->is_value_node_"<<std::endl;
  // 为不带值结点,不符合移除条件,返回原来的结点
  return {A, false};

  // if (!now_key_left.empty()) {
  //   std::shared_ptr<const TrieNode> child_node = nullptr;
  //   for (auto &son : A->children_) {
  //     char seita = son.first;
  //     if (seita == now_key_left[0]) {
  //       child_node = son.second;
  //       break;
  //     }
  //   }
  //   if (child_node == nullptr) {
  //     // 情况是当前字符串为空,则直接返回
  //     return {A, false};
  //   } else {
  //     // 情况是当前字符串还不为空，有可用的结点，顺着这个结点继续删除,并返回一个
  //     auto ret_pair = this->RemoveKey(child_node, now_key_left.substr(1, now_key_left.size() - 1));
  //     if (ret_pair.second) {
  //       auto new_child_node = ret_pair.first;
  //       auto new_children = A->children_;
  //       if (new_child_node != nullptr) {
  //         new_children[now_key_left[0]] = new_child_node;
  //       } else {
  //         new_children.erase(now_key_left[0]);
  //       }
  //       auto new_node_a = std::make_shared<TrieNode>(new_children);

  //       return {new_node_a, true};
  //     } else {
  //       return {A, false};
  //     }
  //   }
  // } else {
  //   if (A->is_value_node_) {
  //     // 直接删除
  //     return {nullptr, true};
  //     // return nullptr;
  //   } else {
  //     // 如果A是一个TrieNode,这里假设CMU让我们直接移除它,虽然可能真实意思是移除带有value的点?
  //     return {nullptr, true};

  //     // if(A->children_.empty()){
  //     //   // 如果A的孩子结点为空，则直接创建叶子结点

  //     //   auto new_leef_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  //     //   return new_leef_node;
  //     //   // return nullptr;
  //     // }else{

  //     //   auto new_leef_node =
  //     //   std::make_shared<TrieNodeWithValue<T>>(A->children_,std::make_shared<T>(std::move(value))); return
  //     //   new_leef_node;
  //     //   // return nullptr;
  //     // }
  //   }
  // }
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  // auto SetValue = [&](std::shared_ptr<const TrieNode> A,std::string_view now_key_left){

  // auto RemoveValue = [&](std::shared_ptr<const TrieNode> A,std::string_view now_key_left){

  // };

  // std::function<std::shared_ptr<TrieNodeWithValue<T>>(std::shared_ptr<const TrieNode>,std::string_view)> GetValue =
  //   [&](std::shared_ptr<const TrieNode> A,std::string_view now_key_left) -> std::shared_ptr<TrieNodeWithValue<T>> {
  //   if(!now_key_left.empty()){
  //     std::shared_ptr<const TrieNode> child_node = nullptr;
  //     for(auto &son:A->children_){
  //       char seita = son.first;
  //       if(seita == now_key_left[0]){
  //         child_node = son.second;
  //         break;
  //       }
  //     }
  //     if(child_node == nullptr){
  //       return nullptr;
  //     }
  //     return GetValue(child_node,now_key_left[1,now_key_left.size()-1]);
  //   }else{
  //     if(!A->is_value_node_){
  //       return nullptr;
  //     }
  //     auto trie_node_with_value = std::shared_ptr<TrieNodeWithValue<T>>(std::move(A->clone()));
  //     // trie_node_with_value
  //     return trie_node_with_value->value_;
  //   }
  // };
  auto ret_pair = this->RemoveKey(this->root_, key);
  if (ret_pair.second) {
    auto new_tree = Trie(ret_pair.first);
    return new_tree;
  }
  return *this;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
