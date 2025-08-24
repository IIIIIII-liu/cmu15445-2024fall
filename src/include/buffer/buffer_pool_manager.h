//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief `BufferPoolManager` 的辅助类，用于管理内存帧（frame）及其相关元数据。
 *
 * 一个 `FrameHeader` 封装了内存帧的头部信息。注意，它不直接存储页面数据，而是通过 `data_` 指针
 * 指向实际的数据。使用 `std::vector` 的目的是为了在开发阶段方便使用 Address Sanitizer 等工具
 * 检测内存越界错误。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();
  /**
   * @brief 该 `FrameHeader` 所代表的内存帧 ID / 索引。
   */
  const frame_id_t frame_id_;
  /**
   * @brief 保护该内存帧的读写锁（读者-写者锁）。
   *
   * 这是实现并发控制的关键。当多个线程需要访问同一个内存帧时，它们会通过这个锁来协调，
   * 确保数据的一致性。
   */
  std::shared_mutex rwlatch_;

  /**
   * @brief 引用计数（pin count）。
   *
   * 这是一个原子变量，表示当前有多少个线程或操作正在使用这个页面。
   * 当 pin count > 0 时，该页面不能被替换出缓冲池。
   */
  std::atomic<size_t> pin_count_;

  /**
   * @brief 脏页标志（dirty flag）。
   *
   * 如果该帧中的数据被修改过，此标志为 true。当一个脏页被替换出缓冲池时，
   * 必须先将其内容写回磁盘以持久化更改。
   */
  bool is_dirty_;

  /**
   * @brief 指向该帧所持有的页面数据的指针。
   *
   * 如果该帧不包含任何页面数据，则所有字节都为 null。
   */
  std::vector<char> data_;
  /**
   * @brief 获取该帧所持有的页面 ID。
   *
   * 这个 ID 用于在缓冲池中唯一标识一个页面。
   */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /**
   * TODO(P1): 你可以在此处添加任何你认为必要的字段或辅助函数。
   *
   * 一个可能的优化是，在 `FrameHeader` 中存储当前页面 ID。这样可以避免在 `BufferPoolManager` 的
   * `page_table_` 中进行额外的哈希表查找。
   */
};

/**
 * @brief `BufferPoolManager` 类的声明。
 *
 * 缓冲池负责将物理数据页在主内存和持久化存储之间来回移动。它同时扮演着缓存的角色，
 * 将常用页面保留在内存中以提高访问速度，并将不常用或“冷”的页面淘汰回磁盘。
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();
  auto Size() const -> size_t;
  /**
   * @brief 创建一个新页面，并将其固定（pin）在缓冲池中。
   * @return 新页面的 ID。
   */
  auto NewPage() -> page_id_t;
  /**
   * @brief 删除一个页面，并将其从缓冲池中移除。
   * @return 如果页面成功删除则返回 true，否则返回 false（例如，如果页面被固定）。
   */
  auto DeletePage(page_id_t page_id) -> bool;
  // 下面的 Checked* 和 *Page 函数是用于获取页面读写锁的封装。
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;

  /**
   * @brief 强制将指定的页面写入磁盘。
   * @return 如果页面成功写入则返回 true，否则返回 false（例如，页面不存在）。
   */
  auto FlushPageUnsafe(page_id_t page_id) -> bool;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPagesUnsafe();
  void FlushAllPages();

  /**
   * @brief 获取指定页面的引用计数。
   * @return 如果页面存在，返回其引用计数；否则返回 `std::nullopt`。
   */
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

 private:
  /** @brief 缓冲池中的帧总数。 */
  const size_t num_frames_;
  /** @brief 下一个要分配的页面 ID。这是一个原子变量，用于并发地生成唯一 ID。 */
  std::atomic<page_id_t> next_page_id_;
  /**
   * @brief 保护缓冲池内部数据结构的锁。
   *
   * 这个互斥锁（mutex）用于保护 `page_table_`、`free_frames_` 和 `replacer_` 等共享资源，
   * 确保在多线程环境下对这些数据结构的访问是线程安全的。
   */
  std::shared_ptr<std::mutex> bpm_latch_;
  /** @brief 存储所有 `FrameHeader` 对象的数组。 */
  std::vector<std::shared_ptr<FrameHeader>> frames_;
  /**
   * @brief 页表，用于记录页面 ID 到内存帧 ID 的映射关系。
   *
   * 这是缓冲池的核心数据结构。通过这个哈希表，我们可以 O(1) 地找到一个页面在内存中的位置。
   */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** @brief 一个空闲帧列表。当有新的页面需要加载时，会优先从这个列表中获取空闲帧。*/
  std::list<frame_id_t> free_frames_;
  /**
   * @brief 替换器，用于查找可被淘汰的页面。
   *
   * 当缓冲池中没有空闲帧时，`BufferPoolManager` 会使用这个替换器，根据 LRU-K 策略来选择
   * 一个最合适的、可以被替换的帧。
   */
  std::shared_ptr<LRUKReplacer> replacer_;
  /** @brief 磁盘调度器，用于异步地将数据页从磁盘加载或写回。 */
  std::shared_ptr<DiskScheduler> disk_scheduler_;
  /**
   * @brief 指向日志管理器的指针。
   *
   * 注意：此成员变量在项目 1 中无需关注，主要用于后续项目实现恢复功能。
   */
  LogManager *log_manager_ __attribute__((__unused__));
  /**
   * TODO(P1): 你可以添加额外的私有成员和辅助函数。
   *
   * 在实现中，你可能会发现很多代码在处理不同访问模式（读/写）时有重复。
   * 建议实现一个通用的辅助函数来获取一个空闲帧（或者通过替换策略获取一个帧）。
   */
};
}  // namespace bustub
