#pragma once
#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * Context：用于一次操作（插入/删除等）期间保存相关的 PageGuard 等上下文信息
 * - header_page_ : 可选的 header page 的写 guard（用于更新 root）
 * - root_page_id_: 操作开始时看到的 root id（用于判断是否为 root）
 * - write_set_/read_set_ : 分别保存被写/被读的页面 guard，便于批量释放/管理
 */
class Context {
 public:
  std::optional<WritePageGuard> header_page_{std::nullopt};
  page_id_t root_page_id_{INVALID_PAGE_ID};
  std::deque<WritePageGuard> write_set_;
  std::deque<ReadPageGuard> read_set_;
  // 记录tree搜索过程中各个结点中的经过的键的index
  std::deque<int> indexes_;
  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * BPlusTree 主类（模板）
 *
 * - 支持唯一键（unique key）
 * - 提供 Insert / Remove / GetValue / iterator（Begin/End）等基础操作
 * - 使用 BufferPoolManager 管理页，使用 PageGuard 管理并发访问
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // 构造：name(索引名)、header_page_id(存放 root id 的页)、buffer_pool_manager、comparator、页的最大槽位数
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // 如果树为空（header->root_page_id == INVALID_PAGE_ID）返回 true
  auto IsEmpty() const -> bool;

  // 插入（唯一键）。重复键返回 false
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // 删除指定键（若不存在直接返回）
  void Remove(const KeyType &key);

  // 点查：把查到的 value 放入 result，找到返回 true
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // 返回当前 root 的 page id（从 header 读取）
  auto GetRootPageId() -> page_id_t;

  // 迭代器相关：Begin()/End()/Begin(key)
  // auto Begin() -> INDEXITERATOR_TYPE;
  // auto End() -> INDEXITERATOR_TYPE;
  // auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto Begin() -> INDEXITERATOR_TYPE {
    Context ctx;

    ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
    auto head_page = guard.As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = head_page->root_page_id_;
    guard.Drop();

    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }

    // 找到最左侧的叶子结点
    ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
    auto page = page_guard.As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal_page = static_cast<const InternalPage *>(page);
      page_id_t page_id = internal_page->ValueAt(0);
      page_guard = bpm_->ReadPage(page_id);
      page = page_guard.As<BPlusTreePage>();
    }
    return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), 0);
  }

  /*
   * Input parameter is void, construct an index iterator representing the end
   * of the key/value pair in the leaf node
   * @return : index iterator
   */
  auto End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

  /*
   * Input parameter is low key, find the leaf page that contains the input key
   * first, then construct index iterator
   * @return : index iterator
   */
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
    Context ctx;

    ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
    auto head_page = guard.As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = head_page->root_page_id_;
    guard.Drop();

    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }

    ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
    auto page = page_guard.As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      int index = KeyBinarySearch(page, key);
      auto internal_page = static_cast<const InternalPage *>(page);
      page_id_t page_id = internal_page->ValueAt(index);
      page_guard = bpm_->ReadPage(page_id);
      page = page_guard.As<BPlusTreePage>();
    }
    // 当前结点为叶子结点时
    int index = KeyBinarySearch(page, key);
    if (index == -1) {
      return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
    }
    return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), index);
  }
  // 打印/绘图（调试）
  void Print(BufferPoolManager *bpm);
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);
  auto DrawBPlusTree() -> std::string;

  // 从文件批量插入/删除（用于测试）
  void InsertFromFile(const std::filesystem::path &file_name);
  void RemoveFromFile(const std::filesystem::path &file_name);
  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  // 将页转为可视化/打印结构
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);
  void PrintTree(page_id_t page_id, const BPlusTreePage *page);
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // 二分查找：在 page 上查找 key（leaf 返回 key 的 index 或 -1；internal 返回 child index）
  auto KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) -> int;

  // 在叶子页上定位插入位置（返回插入下标）
  auto IndexBinarySearchLeaf(LeafPage *page, const KeyType &key) -> int;

  // 借用/合并辅助（处理删除后的重平衡）
  void BorrowFromLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page, int index);
  void BorrowFromRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page, int index);
  void MergeWithLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page, int index);
  void MergeWithRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page, int index);

  // 成员变量
  std::string index_name_;       // 索引名
  BufferPoolManager *bpm_;       // BufferPool 管理器（非拥有）
  KeyComparator comparator_;     // 键比较器
  std::vector<std::string> log_;  // 调试日志（可选）
  int leaf_max_size_;            // 叶页最大容量
  int internal_max_size_;        // 内部页最大容量
  page_id_t header_page_id_;     // header 页的 id（存 root 信息）
};

/**
 * PrintableBPlusTree：把树转换为便于打印的中间结构（仅用于调试）
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  // 层序打印（BFS）
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;
      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');
        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
