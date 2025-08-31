//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include <cassert>

namespace bustub {

auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }

void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

auto BPlusTreePage::GetSize() const -> int { return size_; }

void BPlusTreePage::SetSize(int size) {
  if (size <= 0) {
    size_ = 0;
    return;
  }
  size_ = size;
}

void BPlusTreePage::ChangeSizeBy(int amount) {
  size_ += amount;
  if (size_ < 0) {
    size_ = 0;
  }
}

auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }

void BPlusTreePage::SetMaxSize(int size) {
  if (size <= 0) {
    max_size_ = 0;
    return;
  }
  max_size_ = size;
}
auto BPlusTreePage::IsRootPage() const -> bool { return page_type_ == IndexPageType::INVALID_INDEX_PAGE; }
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsRootPage()) {
    return IsLeafPage() ? 1 : 2;
  }

  if (IsLeafPage()) {
    return (max_size_ - 1 + 1) / 2;
  }

  return (max_size_ - 2 + 1) / 2 + 1;
}

}  // namespace bustub
