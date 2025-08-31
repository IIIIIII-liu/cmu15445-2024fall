//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.h
//
// Identification: src/include/storage/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"
namespace bustub {

// 简化类型别名，便于在实现文件中使用模板实例化时书写
#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

// 内部页头部在父类 BPlusTreePage 中占用的字节数（注释中提到 12 bytes）
#define INTERNAL_PAGE_HEADER_SIZE 12

// 计算内部页最多可容纳的槽位数量（每个槽位包含一个 Key 和一个 Value）
// 这里用整页大小减去页头，然后用单个槽位的字节占用进行除法
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * BPlusTreeInternalPage
 *
 * 内部节点（internal page）类，存储 n 个索引键和 n+1 个子指针（page_id）。
 * 注意：内部节点中的第一个键（key_array_[0]）始终无效（哨兵），真正可用于索引
 * 的键从下标 1 开始。这是为了使 child 指针与键的区间关系更容易维护。
 *
 * 内存布局（逻辑上）:
 * - HEADER （由 BPlusTreePage 提供）
 * - key_array_ (长度为 INTERNAL_PAGE_SLOT_CNT) : key_array_[0] 为无效哨兵
 * - page_id_array_ (长度为 INTERNAL_PAGE_SLOT_CNT) : 存放子页面 id
 *
 * 语义：当页中存有 n 个有效键（不计哨兵）时，page_id_array_ 中将有 n+1 个有效子指针。
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // 禁用默认构造 / 拷贝，源码选择这样是为了避免误用；通常通过 BufferPoolManager 分配并 placement-new
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * @brief 初始化内部页
   * @param max_size 页的最大容量（槽位数），默认使用宏 INTERNAL_PAGE_SLOT_CNT
   *
   * 初始化工作通常包括：
   * - 设置页类型为 INTERNAL_PAGE
   * - 设置当前键个数 size 为 0
   * - 设置最大容量 max_size_
   * - 其他元信息（如 parent_id/page_id）由上层管理器设定
   */
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  /**
   * @brief 获取指定 index 的键（注意 index 为相对于 key_array_ 的索引）
   * @param index 下标，注意：key_array_[0] 为无效哨兵，通常合法查询应从 index>=1 开始
   * @return 返回对应的 KeyType
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   * @brief 在指定 index 处设置键的值
   * @param index 下标，index 必须在合法范围内（通常 >= 1 且 < GetMaxSize()）
   * @param key 要设置的键
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   * @brief 查找某个 value（即子页面 id）在 page_id_array_ 中的下标
   * @param value 要查找的 page_id
   * @return 如果找到返回下标，否则通常返回 -1（或实现选择的 sentinel）
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   * @brief 返回指定下标处的 value（子页面 id）
   * @param index 下标，应在合法范围内
   * @return 返回对应的 ValueType
   */
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }
  /**
   * @brief 仅用于测试，返回一个字符串表示所有键，格式 "(k1,k2,k3,...)"。
   * 注意：由于第一个键无效，ToString 中从 i=1 开始遍历。
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      // 假设 KeyType 提供了 ToString 方法并返回可打印内容
      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // 固定大小的数组用于存放键和值（页内顺序布局）。
  // key_array_[0] 是保留位置（无效），用于对齐 child 指针和键的含义。
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};
}  // namespace bustub
