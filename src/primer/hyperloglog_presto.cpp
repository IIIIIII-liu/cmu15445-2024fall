//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/**
 * @brief 构造函数，初始化 HyperLogLogPresto
 *
 * @tparam KeyType   数据类型（例如 int64_t、std::string）
 * @param n_leading_bits   前导位数量（用于确定桶的数量）
 *
 * dense_bucket_   用于存储较小的计数值（低位部分）
 * overflow_bucket_ 用于存储溢出的高位部分
 * cardinality_    当前估算的基数
 * leading_bits_   前导位个数
 */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : dense_bucket_(1 << (n_leading_bits > 0 ? n_leading_bits : 0), std::bitset<DENSE_BUCKET_SIZE>()),
      cardinality_(0),
      leading_bits_(n_leading_bits > 0 ? n_leading_bits : 0) {}

/**
 * @brief 向 HyperLogLogPresto 添加一个元素
 *
 * 1. 对元素计算哈希值
 * 2. 取前 leading_bits_ 位作为桶索引
 * 3. 从剩余位中统计前导 0 的数量（不包括前导位）
 * 4. 如果当前桶记录的值小于本次计算的值，则更新桶的值（分成高位 overflow 和低位 dense 存储）
 */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hashcode = CalculateHash(val);

  // 将 hashcode 转为二进制表示
  const int size = sizeof(hash_t) * 8;
  std::bitset<size> bits(hashcode);

  // 桶索引：取哈希的高 leading_bits_ 位
  int index = (bits >> (size - leading_bits_)).to_ulong();

  // 统计前导 0 的数量（忽略 leading_bits_ 部分）
  int i = 0;
  uint64_t cnt = 0;
  while (i < (size - leading_bits_) && !bits.test(i)) {
    cnt++;
    i++;
  }

  // 当前桶的已有值（分成高位 overflow 和低位 dense）
  uint64_t overflow_value = overflow_bucket_[index].to_ulong();
  uint64_t dense_value = dense_bucket_[index].to_ulong();

  // 如果本次计算的前导 0 数量更大，则更新
  if ((overflow_value << DENSE_BUCKET_SIZE) + dense_value < cnt) {
    // 更新高位部分（cnt >> DENSE_BUCKET_SIZE 相当于除以 2^DENSE_BUCKET_SIZE）
    overflow_value = cnt >> DENSE_BUCKET_SIZE;

    // 更新低位部分（保留后 DENSE_BUCKET_SIZE 位，相当于 cnt % (1 << DENSE_BUCKET_SIZE)）
    dense_value = cnt & ((1 << DENSE_BUCKET_SIZE) - 1);

    // 写回桶
    overflow_bucket_[index] = overflow_value;
    dense_bucket_[index] = dense_value;
  }
}

/**
 * @brief 计算 HyperLogLogPresto 的基数估计值
 *
 * 公式：cardinality = α * m^2 / Σ(2^-M[i])
 * 其中：
 *   α = 常数（CONSTANT）
 *   m = 桶数量（2^leading_bits_）
 *   M[i] = 桶 i 的值（overflow 高位与 dense 低位拼接）
 */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0;
  uint16_t m = (1 << leading_bits_);
  for (uint16_t i = 0; i < m; i++) {
    // 组合 overflow 高位和 dense 低位
    int tmp = (overflow_bucket_[i].to_ulong() << DENSE_BUCKET_SIZE) + dense_bucket_[i].to_ulong();
    sum += pow(2.0, -tmp);
  }
  cardinality_ = (CONSTANT * m * m) / sum;
}

// 显式模板实例化
template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;

}  // namespace bustub
