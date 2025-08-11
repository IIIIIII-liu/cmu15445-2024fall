//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/**
 * @brief 带参数构造函数
 * @tparam KeyType 需要统计去重数量的键类型
 * @param n_bits   桶索引使用的二进制位数 b（桶数量 m = 2^b）
 *
 * - b_ 表示桶索引的位数（n_bits）
 * - buckets_ 是一个长度为 m 的向量，用来存储每个桶的 "前导零" 最大值
 * - 如果 n_bits <= 0，则默认分配一个桶（避免非法大小）
 */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : cardinality_(0),                                               // 初始基数为 0
      b_(n_bits > 0 ? n_bits : 0),                                   // 桶索引位数，必须大于 0
      buckets_(std::vector<uint64_t>(n_bits > 0 ? 1 << n_bits : 1))  // 分配 2^b 个桶，或至少 1 个
{}

/**
 * @brief 将哈希值转成二进制形式（bitset 表示）
 * @param hash  计算好的哈希值
 * @return      对应的二进制位表示（bitset<BITSET_CAPACITY>）
 *
 * - BITSET_CAPACITY 一般是固定的，比如 64 位
 * - bitset 提供方便的按位操作接口（test, shift, to_ulong 等）
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  // 将哈希值构造为 bitset（低位在右，高位在左）
  std::bitset<BITSET_CAPACITY> bits(hash);
  return bits;
}

/**
 * @brief 计算去掉前 b_ 位后的二进制数的前导零数量 + 1
 * @param bset 整个哈希值对应的二进制位集
 * @return     从最高有效位开始遇到的第一个 '1' 的位置（即前导零个数+1）
 *
 * - b_ 位用于桶索引，剩余位用于计算前导零
 * - 按 HyperLogLog 定义，p = 前导零数 + 1
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  // 从第 BITSET_CAPACITY - 1 - b_ 位开始（跳过桶索引位）
  int i = BITSET_CAPACITY - 1 - b_;
  uint64_t ans = 1;                  // 默认至少为 1（如果第一位就是 1，则返回 1）
  while (i >= 0 && !bset.test(i)) {  // 从高位往低位找第一个 1
    ans++;
    i--;
  }
  return ans;
}

/**
 * @brief 向 HyperLogLog 添加一个元素
 * @param val  要加入的数据
 *
 * - 第一步：计算 val 的哈希值
 * - 第二步：转成二进制
 * - 第三步：分离出桶索引和剩余位
 * - 第四步：更新对应桶的前导零最大值
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  // 计算哈希
  hash_t hashcode = CalculateHash(val);

  // 转二进制
  std::bitset<BITSET_CAPACITY> bits = ComputeBinary(hashcode);

  // 计算前导零数量（不包括桶索引位）
  uint64_t p = PositionOfLeftmostOne(bits);

  // 桶索引 = 哈希值的高 b_ 位
  uint64_t index = (bits >> (BITSET_CAPACITY - b_)).to_ulong();

  // 更新该桶的最大前导零值
  buckets_[index] = std::max(buckets_[index], p);
}

/**
 * @brief 根据桶的统计值估算集合基数（去重数量）
 *
 * - 使用 HyperLogLog 的公式：
 *   E = α_m * m^2 / (Σ 2^{-M[j]})
 *   其中 α_m 为常数（CONSTANT），M[j] 为第 j 个桶的前导零最大值
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0;
  uint64_t m = (1 << b_);  // 桶的数量

  // 累加每个桶的 2^{-M[j]}
  for (uint64_t i = 0; i < m; i++) {
    sum += pow(2.0, -static_cast<double>(buckets_[i]));
  }

  // 计算估算基数
  cardinality_ = (CONSTANT * m * m) / sum;
}

// 显式模板实例化，避免链接错误
template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
