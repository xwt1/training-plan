//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
// 为什么声明函数的时候不加INDEX_TEMPLATE_ARGUMENTS，而在实现的时候加上呢：
// 在C++中，类模板（class template）通常在类定义时使用模板参数（template parameters）进行声明，例如：

// template <typename T>
// class MyClass {
// public:
//     T data;
//     // 构造函数、成员函数等
// };

// 在类定义中，`template <typename T>` 部分指定了模板参数
// `T`，它表示这个类模板可以接受不同的类型作为模板参数，从而创建不同的类类型的实例。例如，你可以实例化 `MyClass` 为
// `MyClass<int>` 或 `MyClass<double>`，这将创建不同类型的类对象。

// 然而，有时候你可能会在类定义之后的某些成员函数或非成员函数中需要再次使用模板参数
// `T`。在这种情况下，你需要在函数定义中重新声明模板参数 `T`，即使用 `template<typename
// T>`，以告诉编译器该函数也是一个模板函数，使用之前定义的模板参数 `T`，例如：

// template <typename T>
// class MyClass {
// public:
//     T data;

//     void someFunction(T value) {
//         // 在函数中使用 T 类型的变量
//     }
// };

// template <typename T>
// void someOtherFunction(T value) {
//     // 在非类模板函数中也使用 T 类型的变量
// }

// 这就是在函数定义时带上 `template<typename T>`
// 的意义：它告诉编译器这个函数也是一个模板函数，它的模板参数与之前定义的类模板相同，因此可以使用相同的模板参数
// `T`。这使你能够在函数中使用与类模板相同的类型 `T`，从而保持一致性。如果省略了 `template<typename
// T>`，编译器可能无法识别 `T` 是什么类型。

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = this->array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { this->array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return this->array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const page_id_t &value) {
  this->array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() const -> const MappingType * { return this->array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetArrayAt(int index, std::pair<KeyType, page_id_t> value) {
  this->array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetMinSize() const -> int{
  int max_size = this->GetMaxSize();
  return (max_size & 1) != 0?max_size/2 +1 : max_size / 2;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteAValue(int pos) {
  for(int i = pos +1 ;i< this->GetSize();i++){
    this->SetArrayAt(i-1,std::make_pair(this->KeyAt(i),this->ValueAt(i)));
  }
  this->IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
