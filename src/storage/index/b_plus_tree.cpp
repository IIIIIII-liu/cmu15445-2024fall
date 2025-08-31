//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

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
  // 初始化 header，表示空树
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 判断树是否为空：检查 header 的 root_page_id
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

//
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) -> int {
  int l;
  int r;

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<const LeafPage *>(page);
    l = 0, r = leaf_page->GetSize() - 1;
    while (l <= r) {
      int mid = (l + r) >> 1;
      if (comparator_(key, leaf_page->KeyAt(mid)) == 0) {
        return mid;
      }
      if (comparator_(key, leaf_page->KeyAt(mid)) < 0) {
        r = mid - 1;
      } else {
        l = mid + 1;
      }
    }
  } else {
    auto internal_page = static_cast<const InternalPage *>(page);
    // 注意这里 l 要为 1
    l = 1, r = internal_page->GetSize() - 1;
    int size = internal_page->GetSize();
    // 内部节点的一个特殊情况，考虑key小于结点中第一个键的情况
    if (comparator_(key, internal_page->KeyAt(l)) < 0) {
      return 0;
    }
    while (l <= r) {
      int mid = (l + r) >> 1;
      if (comparator_(internal_page->KeyAt(mid), key) <= 0) {
        if (mid + 1 >= size || comparator_(internal_page->KeyAt(mid + 1), key) > 0) {
          return mid;
        }
        l = mid + 1;
      } else {
        r = mid - 1;
      }
    }
  }

  return -1;
}

/**
 * insert时查找叶子结点中插入位置的函数
 * 语义：返回插入位置 i（0..size），当无法确定时返回 -1（调用方通常把 -1 当作失败）
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IndexBinarySearchLeaf(LeafPage *page, const KeyType &key) -> int {
  int size = page->GetSize();
  // 空叶子直接返回 0（插入到第 0 个位置）
  if (size == 0) {
    return 0;
  }

  int l = 0;
  int r = size - 1;
  // 如果比最小值更小，插到最左
  if (comparator_(key, page->KeyAt(0)) < 0) {
    return 0;
  }

  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator_(page->KeyAt(mid), key) < 0) {
      if (mid + 1 >= size || comparator_(page->KeyAt(mid + 1), key) >= 0) {
        return mid + 1;
      }
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }

  // 理论上不应该到这里；作为防御性处理返回 -1（调用处多数会判定失败）
  return -1;
}

/**
 * 向左sibling结点借用键值的函数
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                                    int index) {
  // parent_page必然是内部结点，先将其进行转换
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  int size = page->GetSize();
  int left_size = left_page->GetSize();

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto left_leaf_page = static_cast<LeafPage *>(left_page);

    // 从左边开始遍历移动还是从右边，一定要想清楚，不然会出错！
    for (int i = size - 1; i >= 0; i--) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));
    }
    leaf_page->SetKeyAt(0, left_leaf_page->KeyAt(left_size - 1));
    leaf_page->SetValueAt(0, left_leaf_page->ValueAt(left_size - 1));
    left_leaf_page->SetSize(left_size - 1);
    leaf_page->SetSize(size + 1);
    parent_internal_page->SetKeyAt(index, leaf_page->KeyAt(0));
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto left_internal_page = static_cast<InternalPage *>(left_page);

    for (int i = size - 1; i >= 0; i--) {
      if (i > 0) {
        internal_page->SetKeyAt(i + 1, internal_page->KeyAt(i));
      }
      internal_page->SetValueAt(i + 1, internal_page->ValueAt(i));
    }
    internal_page->SetKeyAt(1, parent_internal_page->KeyAt(index));
    internal_page->SetValueAt(0, left_internal_page->ValueAt(left_size - 1));
    parent_internal_page->SetKeyAt(index, left_internal_page->KeyAt(left_size - 1));
    left_internal_page->SetSize(left_size - 1);
    internal_page->SetSize(size + 1);
  }
}

/**
 * 向右sibling结点借用键值的函数
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                                     int index) {
  // parent_page必然是内部结点，先将其进行转换
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  int size = page->GetSize();
  int right_size = right_page->GetSize();

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);

    leaf_page->SetKeyAt(size, right_leaf_page->KeyAt(0));
    leaf_page->SetValueAt(size, right_leaf_page->ValueAt(0));
    for (int i = 0; i < right_size - 1; i++) {
      right_leaf_page->SetKeyAt(i, right_leaf_page->KeyAt(i + 1));
      right_leaf_page->SetValueAt(i, right_leaf_page->ValueAt(i + 1));
    }
    right_leaf_page->SetSize(right_size - 1);
    leaf_page->SetSize(size + 1);
    parent_internal_page->SetKeyAt(index + 1, right_leaf_page->KeyAt(0));
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);

    internal_page->SetKeyAt(size, parent_internal_page->KeyAt(index + 1));
    internal_page->SetValueAt(size, right_internal_page->ValueAt(0));
    parent_internal_page->SetKeyAt(index + 1, right_internal_page->KeyAt(1));
    for (int i = 0; i < right_size; i++) {
      if (i > 0) {
        right_internal_page->SetKeyAt(i, right_internal_page->KeyAt(i + 1));
      }
      right_internal_page->SetValueAt(i, right_internal_page->ValueAt(i + 1));
    }
    right_internal_page->SetSize(right_size - 1);
    internal_page->SetSize(size + 1);
  }
}

/**
 * 与左sibling结点合并的函数
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                                   int index) {
  int left_size = left_page->GetSize();
  int size = page->GetSize();
  int parent_size = parent_page->GetSize();
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto left_leaf_page = static_cast<LeafPage *>(left_page);
    for (int i = 0; i < size; i++) {
      left_leaf_page->SetKeyAt(i + left_size, leaf_page->KeyAt(i));
      left_leaf_page->SetValueAt(i + left_size, leaf_page->ValueAt(i));
    }
    // 随时记得更新结点的size
    left_leaf_page->SetSize(left_size + size);
    // 要记得更新next_page_id ！很容易漏
    // 在迭代器部分会检测这里是否正确进行了更新
    left_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto left_internal_page = static_cast<InternalPage *>(left_page);
    KeyType middle_key = parent_internal_page->KeyAt(index);
    left_internal_page->SetKeyAt(left_size, middle_key);
    left_internal_page->SetValueAt(left_size, internal_page->ValueAt(0));
    for (int i = 1; i < size; i++) {
      left_internal_page->SetKeyAt(i + left_size, internal_page->KeyAt(i));
      left_internal_page->SetValueAt(i + left_size, internal_page->ValueAt(i));
    }
    left_internal_page->SetSize(left_size + size);
  }

  // 处理父结点
  for (int i = index; i < parent_size - 1; i++) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));
  }
  parent_internal_page->SetSize(parent_size - 1);
}

/**
 * 与右sibling结点合并的函数
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                                    int index) {
  int right_size = right_page->GetSize();
  int size = page->GetSize();
  int parent_size = parent_page->GetSize();
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);
    for (int i = 0; i < right_size; i++) {
      leaf_page->SetKeyAt(i + size, right_leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + size, right_leaf_page->ValueAt(i));
    }
    // 随时记得更新结点的size
    leaf_page->SetSize(right_size + size);
    // 要记得更新next_page_id ！很容易漏
    // 在迭代器中会检测这里是否更新
    leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);
    KeyType middle_key = parent_internal_page->KeyAt(index + 1);
    // 之前对于size的设置出现了问题，导致出现了size为0的情况进行key的设置，触发了exception
    internal_page->SetKeyAt(size, middle_key);
    internal_page->SetValueAt(size, right_internal_page->ValueAt(0));
    for (int i = 1; i < right_size; i++) {
      internal_page->SetKeyAt(i + size, right_internal_page->KeyAt(i));
      internal_page->SetValueAt(i + size, right_internal_page->ValueAt(i));
    }
    internal_page->SetSize(right_size + size);
  }

  // 处理父结点
  for (int i = index + 1; i < parent_size - 1; i++) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));
  }
  parent_internal_page->SetSize(parent_size - 1);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_page->root_page_id_;
  guard.Drop();

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  /* 当前结点为内部结点时 */
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    if (index == -1) {
      return false;
    }
    auto internal_page = static_cast<const InternalPage *>(page);
    page_id_t page_id = internal_page->ValueAt(index);
    ctx.read_set_.push_back(bpm_->ReadPage(page_id));
    page = ctx.read_set_.back().As<BPlusTreePage>();
    ctx.read_set_.pop_front();
  }

  /* 当前结点为叶子结点时 */
  int index = KeyBinarySearch(page, key);
  if (index == -1) {
    return false;
  }
  auto leaf_page = static_cast<const LeafPage *>(page);
  result->push_back(leaf_page->ValueAt(index));

  // (void)ctx;
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;

  // 获取 header 的写锁（为了可能创建根并在并发情形下保护 root_page_id 的读取/写入）
  WritePageGuard tmp_head_guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(tmp_head_guard));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  // 注意：这里拿 header 写锁的目的是为了在必要时修改 root_page_id 或保护对 root 的读取（避免竞态）

  /* (1) 如果 tree 是空的，创建根叶节点并返回 */
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t root_page_id = bpm_->NewPage();
    WritePageGuard root_guard = bpm_->WritePage(root_page_id);
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->SetSize(1);

    auto head_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    head_page->root_page_id_ = root_page_id;
    return true;
  }

  /* (2) tree 非空，先走乐观路径（读锁向下走，遇叶子再升级为写锁） */
  BPlusTreePage *op_write_page = nullptr;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto op_page = ctx.read_set_.back().As<BPlusTreePage>();

  // 如果 root 本身就是叶子，直接把读 guard 换成写 guard（升级）
  if (op_page->IsLeafPage()) {
    ctx.read_set_.pop_back();
    ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
    op_page = ctx.write_set_.back().As<BPlusTreePage>();
  }

  // 释放 header（乐观路径里 header 写锁在到达叶子之前可以释放，以便提高并发）
  ctx.header_page_ = std::nullopt;
  page_id_t page_id = ctx.root_page_id_;

  // lambda: 在已经持有 leaf 的写锁情况下插入（不处理叶满的情况）
  auto insert_into_locked_leaf = [&](LeafPage *leaf) -> bool {
    int insert_index = IndexBinarySearchLeaf(leaf, key);
    // 返回 -1 表示没有满足条件的插入位置（例如与已存在 key 相等或其它异常），调用方处理
    if (insert_index == -1) {
      return false;
    }
    if (insert_index < leaf->GetSize() && comparator_(leaf->KeyAt(insert_index), key) == 0) {
      return false;
    }
    int cur_size = leaf->GetSize();
    for (int i = cur_size; i > insert_index; --i) {
      leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
      leaf->SetValueAt(i, leaf->ValueAt(i - 1));
    }
    leaf->SetKeyAt(insert_index, key);
    leaf->SetValueAt(insert_index, value);
    leaf->SetSize(cur_size + 1);
    return true;
  };

  // 向下找到叶子（在到叶子时从读升级为写）
  while (!op_page->IsLeafPage()) {
    int index = KeyBinarySearch(op_page, key);
    if (index == -1) {
      return false;
    }
    auto internal_page = static_cast<const InternalPage *>(op_page);
    page_id = internal_page->ValueAt(index);

    // latch crabbing：先拿 child 的读 guard
    ctx.read_set_.push_back(bpm_->ReadPage(page_id));
    op_page = ctx.read_set_.back().As<BPlusTreePage>();
    // 若 child 是叶子，则升级：释放刚才的读 guard，获取写 guard（在父锁仍然存在的情况下）
    if (op_page->IsLeafPage()) {
      ctx.read_set_.pop_back();                            // 释放 child 的读 guard（后面换成写 guard）
      ctx.write_set_.push_back(bpm_->WritePage(page_id));  // 获取 child 的写 guard
      op_page = ctx.write_set_.back().As<BPlusTreePage>();
    }
    // 释放最早的读 guard（向下移动）
    ctx.read_set_.pop_front();
  }

  // 此时 ctx.write_set_.back() 应为目标叶的写 guard（若 root 为叶则已在上面升级）
  op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();

  // 乐观路径：如果叶子有空间，直接插入并返回
  if (op_write_page->GetSize() < op_write_page->GetMaxSize()) {
    auto leaf_page = static_cast<LeafPage *>(op_write_page);
    // if (insert_into_locked_leaf(leaf_page)) {
    //   return true;
    // }
    // // 若插入失败（例如 key 已存在或插入点为 -1），直接返回 false
    // return false;
    return insert_into_locked_leaf(leaf_page);
  }

  // 若乐观路径不可行（叶子已满或者插入失败），清理写集合并进入悲观路径
  ctx.write_set_.clear();

  /* (2.2) 悲观路径：从 header 开始自上向下抓写锁（latch crabbing），保证没有空窗期 */
  WritePageGuard head_guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(head_guard));
  // 重新读取 root_page_id（有可能其他线程在乐观路径时创建了新的 root）
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  // 抓 root 的写锁
  WritePageGuard write_page_guard = bpm_->WritePage(ctx.root_page_id_);
  auto page = write_page_guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(write_page_guard));

  // latch grabbing：如果当前 page 有空位，则释放 header 以增加并发性
  if (page->GetSize() < page->GetMaxSize()) {
    ctx.header_page_ = std::nullopt;
  }

  // 自上而下抓写锁直到叶子（路径上的内部结点写锁会根据是否需要继续抓保持或释放）
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    if (index == -1) {
      return false;
    }
    auto internal_page = static_cast<InternalPage *>(page);
    page_id_t next_pid = internal_page->ValueAt(index);
    ctx.write_set_.push_back(bpm_->WritePage(next_pid));
    ctx.indexes_.push_back(index);
    page = ctx.write_set_.back().AsMut<BPlusTreePage>();

    // 如果当前 page（刚拿到的 child）尚有空间，则可以释放路径上除当前之外的写锁（提高并发）
    if (page->GetSize() < page->GetMaxSize()) {
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
      }
    }
  }

  // 现在 page 指向叶子并且持有写锁
  auto leaf_page = static_cast<LeafPage *>(page);
  int insert_index = IndexBinarySearchLeaf(leaf_page, key);
  if (insert_index == -1) {
    return false;
  }
  if (insert_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_index), key) == 0) {
    return false;
  }

  // 如果叶子未满，直接插入
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    int size = leaf_page->GetSize();
    for (int i = size; i > insert_index; --i) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetKeyAt(insert_index, key);
    leaf_page->SetValueAt(insert_index, value);
    leaf_page->SetSize(size + 1);
    // 释放当前叶子的写锁（父节点上的写锁已在循环中被释放）
    ctx.write_set_.pop_front();
    return true;
  }

  /* 若叶子已满，执行 split（悲观路径） */
  int first_size = (leaf_page->GetMaxSize() + 2) / 2;          // 左侧元素数量（向上取整）
  int second_size = leaf_page->GetMaxSize() + 1 - first_size;  // 右侧元素数量

  page_id_t new_leaf_id = bpm_->NewPage();
  WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  ctx.write_set_.push_back(std::move(new_leaf_guard));
  new_leaf_page->Init(leaf_max_size_);

  // 设置大小与 next 指针
  new_leaf_page->SetSize(second_size);
  leaf_page->SetSize(first_size);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_id);

  // 把原数据根据 insert_index 分配到两个叶子（注意各自索引偏移）
  if (insert_index < first_size) {
    // 新叶子复制原叶右侧 (从 first_size - 1 开始) 的元素（注意索引偏移）
    for (int i = 0; i < second_size; ++i) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + first_size - 1));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(i + first_size - 1));
    }
    // 在老叶子内部移动并插入新元素
    for (int i = first_size - 1; i > insert_index; --i) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetKeyAt(insert_index, key);
    leaf_page->SetValueAt(insert_index, value);
  } else {
    // 新元素位于新叶子
    for (int i = 0; i < insert_index - first_size; ++i) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + first_size));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(i + first_size));
    }
    new_leaf_page->SetKeyAt(insert_index - first_size, key);
    new_leaf_page->SetValueAt(insert_index - first_size, value);
    for (int i = insert_index - first_size + 1; i < second_size; ++i) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + first_size - 1));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(i + first_size - 1));
    }
  }

  /* 叶子分裂之后，准备向上插入 internal key */
  KeyType insert_key = new_leaf_page->KeyAt(0);
  // 释放叶子上的写锁（先释放新叶，再释放旧叶）
  ctx.write_set_.pop_back();
  ctx.write_set_.pop_back();

  page_id_t first_split_page_id = ctx.root_page_id_;
  page_id_t second_split_page_id = new_leaf_id;
  bool new_root_flag = true;

  // 向上层传播：使用 ctx.write_set_ 和 ctx.indexes_ 来逐层处理（从父节点开始）
  while (!ctx.write_set_.empty()) {
    int parent_insert_index = ctx.indexes_.back() + 1;
    auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    int size = internal_page->GetSize();

    // 父节点有空间，直接插入
    if (size < internal_page->GetMaxSize()) {
      for (int i = size; i > parent_insert_index; --i) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
      }
      internal_page->SetKeyAt(parent_insert_index, insert_key);
      internal_page->SetValueAt(parent_insert_index, second_split_page_id);
      internal_page->SetSize(size + 1);
      new_root_flag = false;
      ctx.write_set_.clear();
      ctx.indexes_.clear();
      break;
    }

    // 父节点也满了，继续分裂父节点
    int p_first_size = (internal_page->GetMaxSize() + 2) / 2;
    int p_second_size = internal_page->GetMaxSize() + 1 - p_first_size;
    page_id_t new_internal_id = bpm_->NewPage();
    WritePageGuard new_internal_guard = bpm_->WritePage(new_internal_id);
    auto new_internal_page = new_internal_guard.AsMut<InternalPage>();
    ctx.write_set_.push_back(std::move(new_internal_guard));
    new_internal_page->Init(internal_max_size_);
    new_internal_page->SetSize(p_second_size);
    internal_page->SetSize(p_first_size);

    // 中间 key 要向上移动（不保留在两个子节点中）
    if (parent_insert_index < p_first_size) {
      KeyType tmp_key = internal_page->KeyAt(p_first_size - 1);
      for (int i = 0; i < p_second_size; ++i) {
        if (i > 0) {
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(i + p_first_size - 1));
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(i + p_first_size - 1));
      }
      // 在老节点插入新的 key/value
      for (int i = p_first_size - 1; i > parent_insert_index; --i) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
      }
      internal_page->SetKeyAt(parent_insert_index, insert_key);
      internal_page->SetValueAt(parent_insert_index, second_split_page_id);
      // 更新 insert_key（上移的 key）
      insert_key = tmp_key;
    } else {
      // 要插入的位置在新 internal 节点
      for (int i = 0; i < parent_insert_index - p_first_size; ++i) {
        if (i > 0) {
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(i + p_first_size));
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(i + p_first_size));
      }
      KeyType tmp_key;
      if (parent_insert_index > p_first_size) {
        new_internal_page->SetKeyAt(parent_insert_index - p_first_size, insert_key);
        tmp_key = internal_page->KeyAt(p_first_size);
      } else {
        // parent_insert_index == p_first_size
        tmp_key = insert_key;
      }
      new_internal_page->SetValueAt(parent_insert_index - p_first_size, second_split_page_id);
      for (int i = parent_insert_index - p_first_size + 1; i < p_second_size; ++i) {
        new_internal_page->SetKeyAt(i, internal_page->KeyAt(i + p_first_size - 1));
        new_internal_page->SetValueAt(i, internal_page->ValueAt(i + p_first_size - 1));
      }
      insert_key = tmp_key;
    }

    // 更新 second_split_page_id 并释放新 internal 页的 guard
    second_split_page_id = new_internal_id;
    ctx.write_set_.pop_back();
    // 释放当前处理的老 internal 页的 guard，并更新 indexes
    ctx.write_set_.pop_back();
    ctx.indexes_.pop_back();
  }

  // 如果需要创建新的 root（向上冒到根）
  if (new_root_flag) {
    page_id_t new_root_id = bpm_->NewPage();
    WritePageGuard new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    ctx.write_set_.push_back(std::move(new_root_guard));

    new_root_page->Init(internal_max_size_);
    // internal page 的 size_ 表示 value 的数量（key 数 + 1）
    new_root_page->SetSize(2);
    new_root_page->SetKeyAt(1, insert_key);
    new_root_page->SetValueAt(0, first_split_page_id);
    new_root_page->SetValueAt(1, second_split_page_id);

    auto head_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    head_page->root_page_id_ = new_root_id;
    ctx.write_set_.clear();
  }

  return true;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;

  ReadPageGuard read_head_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = read_head_guard.As<BPlusTreeHeaderPage>()->root_page_id_;

  /* (1) 如果tree是空的 */
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  /* (2) 如果tree不是空的*/
  /* (2.1) 乐观锁 */
  /* (2.1.1) 首先找到要进行删除操作的叶子结点 */
  BPlusTreePage *op_write_page = nullptr;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto op_page = ctx.read_set_.back().As<BPlusTreePage>();
  // 如果root结点为叶子结点，则将其升级为写锁。这里存在时间空窗，但有header结点锁未释放，提供了线程保护
  if (op_page->IsLeafPage()) {
    ctx.read_set_.pop_back();
    ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
    op_page = ctx.write_set_.back().As<BPlusTreePage>();
  }
  read_head_guard.Drop();
  page_id_t page_id = ctx.root_page_id_;

  while (!op_page->IsLeafPage()) {
    int index = KeyBinarySearch(op_page, key);
    if (index == -1) {
      return;
    }
    auto internal_page = static_cast<const InternalPage *>(op_page);
    page_id = internal_page->ValueAt(index);
    // 要记得维护ctx对象
    // latch crabbing
    ctx.read_set_.push_back(bpm_->ReadPage(page_id));
    op_page = ctx.read_set_.back().As<BPlusTreePage>();
    // 如果当前结点为叶子结点，则在其父结点锁未被释放的情况下，进行读锁向写锁的升级
    // 父结点锁未被释放，保证读写锁升级过程的线程安全
    if (op_page->IsLeafPage()) {
      ctx.read_set_.pop_back();
      ctx.write_set_.push_back(bpm_->WritePage(page_id));
      op_page = ctx.write_set_.back().As<BPlusTreePage>();
    }
    ctx.read_set_.pop_front();
  }

  op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();

  // 如果叶子结点无需合并以及借用，则获取WritePageGuard后直接delete
  if (op_write_page->GetSize() > op_write_page->GetMinSize()) {
    int delete_index = KeyBinarySearch(op_write_page, key);
    if (delete_index == -1) {
      return;
    }

    int size = op_write_page->GetSize();
    auto leaf_page = static_cast<LeafPage *>(op_write_page);
    for (int i = delete_index; i < size - 1; i++) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
    }
    // 随时记得修改结点size_
    leaf_page->SetSize(size - 1);
    return;
  }
  ctx.write_set_.clear();

  /* (2.2) 悲观锁 latch grabbing */
  /* (2.2.1) 首先找到要进行删除操作的叶子结点 */
  WritePageGuard head_guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(head_guard));
  // 重新获得一次root结点page
  // id，之前在test中出现过多线程问题，主要问题在于其他线程创建了新root结点，header被修改了root_page_id，但是这里用的root_page_id依旧是函数最开始时获取的
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  WritePageGuard write_page_guard = bpm_->WritePage(ctx.root_page_id_);
  auto page = write_page_guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(write_page_guard));
  // latch grabbing
  // delete函数中，root结点最小值不是其他结点的minSize，其size需要大于2（即key数量大于1）
  if (page->GetSize() > 2) {
    ctx.header_page_ = std::nullopt;
  }

  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    if (index == -1) {
      return;
    }
    auto internal_page = static_cast<InternalPage *>(page);
    page_id_t page_id = internal_page->ValueAt(index);
    // 要记得维护ctx对象
    ctx.write_set_.push_back(bpm_->WritePage(page_id));
    // 个人觉得需要在context类中加入存放内部结点搜索位置index的数组
    ctx.indexes_.push_back(index);
    page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    if (page->GetSize() > page->GetMinSize()) {
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
      }
    }
  }

  /* (2.2.2) 之后找到要删除key的位置 */
  int delete_index = KeyBinarySearch(page, key);
  if (delete_index == -1) {
    return;
  }

  // 统一先将对应的key和value删除
  int size = page->GetSize();
  auto leaf_page = static_cast<LeafPage *>(page);
  for (int i = delete_index; i < size - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  // 随时记得修改结点size_
  page->SetSize(size - 1);

  // 当前操作结点的page id，在原root变为空要被删除时，也是新root结点的page id，
  page_id_t now_page_id = INVALID_PAGE_ID;

  /* (2.2.3)
   * 若删除后的结点大于等于半满，则完成删除操作;若删除后的叶子结点小于半满，则先判断能否从sibling结点中借key。若可借则借完后修改父结点即可；若不能借则进行与sibling的合并，之后再向上判断内部结点情况。内部结点同理。由此设计向上迭代的处理过程
   */
  while (!ctx.write_set_.empty()) {
    // 如果此时结点为root结点
    if (ctx.write_set_.size() == 1) {
      auto root_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      // 如果root结点为叶子结点，则单独处理。如果root不为空，则直接return，如果为空，则重新设置root page id
      if (root_page->IsLeafPage()) {
        if (root_page->GetSize() == 0) {
          auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
          header_page->root_page_id_ = INVALID_PAGE_ID;
        }
        return;
      }
      // 如果此时root结点为内部结点且为空，则将原root结点删除，且修改root结点的page id
      // size为1时，没有key存在，只有一个value，此时root为不合法状态，同样需要删除
      if (root_page->GetSize() <= 1) {
        ctx.write_set_.pop_back();
        bpm_->DeletePage(ctx.root_page_id_);
        auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = now_page_id;
      }
      // 若root结点不为空，则不用管minSize的约束，直接return
      return;
    }

    // 若删除后的结点大于等于半满，则完成删除操作。这里相当于递归的出口
    if (page->GetSize() >= page->GetMinSize()) {
      return;
    }

    // 使用反向迭代器获取deque倒数第二个元素，即当前处理元素的父结点
    auto it = ctx.write_set_.rbegin();
    ++it;
    auto parent_page = it->AsMut<InternalPage>();
    int index = ctx.indexes_.back();

    // 先判断是否可以左借用
    if (index > 0) {
      WritePageGuard left_guard = bpm_->WritePage(parent_page->ValueAt(index - 1));
      auto left_page = left_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(left_guard));
      if (left_page->GetSize() > left_page->GetMinSize()) {
        BorrowFromLeft(page, left_page, parent_page, index);
        return;
      }
      // 如果没有进行左借用，记得把left_guard释放，不再占用对应页面
      ctx.write_set_.pop_back();
    }

    // 再判断是否可以右借用
    if (index < parent_page->GetSize() - 1) {
      WritePageGuard right_guard = bpm_->WritePage(parent_page->ValueAt(index + 1));
      auto right_page = right_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(right_guard));
      if (right_page->GetSize() > right_page->GetMinSize()) {
        BorrowFromRight(page, right_page, parent_page, index);
        return;
      }
      // 如果没有进行右借用，记得把right_guard释放，不再占用对应页面
      ctx.write_set_.pop_back();
    }

    // 不可借用，则先判断是否可以左合并
    if (index > 0) {
      WritePageGuard left_guard = bpm_->WritePage(parent_page->ValueAt(index - 1));
      auto left_page = left_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(left_guard));
      MergeWithLeft(page, left_page, parent_page, index);
      now_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      // 将被合并的页面delete
      page_id_t page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      bpm_->DeletePage(page_id);
    } else {
      // 若不可左合并（即index为0时），则进行右合并
      WritePageGuard right_guard = bpm_->WritePage(parent_page->ValueAt(index + 1));
      auto right_page = right_guard.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(right_guard));
      MergeWithRight(page, right_page, parent_page, index);
      // 将被合并的页面delete，注意这里删除的是右边的结点
      page_id_t page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      now_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      bpm_->DeletePage(page_id);
    }
    // 之前漏了这句，出现了很严重的bug，导致读取的page id错误，由此导致了递归加锁的情况
    ctx.indexes_.pop_back();
    page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  }

  // (void)ctx;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  // 返回 header 中记录的 root page id
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/* explicit template instantiations */
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
