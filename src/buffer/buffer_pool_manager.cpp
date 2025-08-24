//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <future>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief FrameHeader 构造函数：为每个帧初始化默认值
 *
 * 说明：FrameHeader 保存单个内存帧的元数据（frame id、页数据缓冲区、页 id、引用计数、脏页标志等）
 * @param frame_id 构造的帧索引
 */
FrameHeader::FrameHeader(frame_id_t frame_id)
    : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0), page_id_(INVALID_PAGE_ID) {
  Reset();  // 将内部字段重置为初始状态
}

/**
 * @brief 获取对帧数据的只读原始指针
 * @return const char* 指向不可变数据的指针
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief 获取对帧数据的可写原始指针
 * @return char* 指向可变数据的指针
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief 重置 FrameHeader 的字段为默认值
 *
 * 将 page_id 置为 INVALID_PAGE_ID，清零数据区，将 pin_count 置 0，清除脏页标志
 */
void FrameHeader::Reset() {
  page_id_ = INVALID_PAGE_ID;
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief BufferPoolManager 构造函数：初始化缓冲池管理器的核心结构
 *
 * 参数说明：
 *  - num_frames: 缓冲池帧数
 *  - disk_manager: 磁盘管理器（用于实际读写磁盘）
 *  - k_dist: LRU-K 算法中后向距离参数
 *  - log_manager: 日志管理器（P1 阶段可以忽略）
 *
 * 实现要点：分配 frames 向量、page_table 容器，初始化 free_frames 列表
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // 这里用全局锁保护初始化步骤（尽管构造期间通常没有并发问题）
  std::scoped_lock latch(*bpm_latch_);
  // 原子自增计数器初始化
  next_page_id_.store(0);
  // 预分配内存，提高性能
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);

  // 初始化所有帧头，并将所有帧加入空闲列表
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief 析构函数（默认）
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief 返回缓冲池中的帧数量
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief 在磁盘上分配一个新页并将其映射到缓冲池中的一个帧（如果可能）
 *
 * 实现要点：
 *  1. 使用全局锁保护缓冲池元数据访问（page_table_、free_frames_ 等）
 *  2. 如果有空闲帧，直接使用；否则从替换器中选择一个可淘汰的帧并执行淘汰逻辑
 *  3. 对被淘汰帧若为脏页则写回磁盘（通过 disk_scheduler_ 异步调度并等待结果）
 *  4. 为新页分配新的 page_id（使用原子自增）并更新 page_table_
 *
 * @return 新分配的 page id；若无法分配则返回 INVALID_PAGE_ID
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // LOG_DEBUG("获取新页");
  // 1. 加锁保护缓冲池结构
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  // 2. 查找可用内存帧（优先使用 free_frames_）
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // 若无空闲帧，从替换器中淘汰一帧
    auto maybe_frame_id = replacer_->Evict();
    if (!maybe_frame_id.has_value()) {
      // 无法淘汰，内存不足
      // LOG_ERROR("淘汰失败，无法分配新页");
      return INVALID_PAGE_ID;
    }
    // 获取被淘汰的帧
    frame_id = maybe_frame_id.value();
    auto &frame = frames_[frame_id];
    // 对帧加写锁（独占）以安全处理脏页写回和重置元数据
    std::lock_guard<std::shared_mutex> wtire_lock(frame->rwlatch_);

    // 如果是脏页，需要写回磁盘
    // 使用 disk_scheduler_ 调度写请求（通过 promise/future 同步等待结果）
    std::promise<bool> write_promise;
    auto write_future = write_promise.get_future();
    disk_scheduler_->Schedule({true, frame->GetDataMut(), frame->page_id_, std::move(write_promise)});
    if (!write_future.get()) {
      LOG_ERROR("Disk write failed during page eviction");
      return INVALID_PAGE_ID;
    }

    frame->is_dirty_ = false;

    // 如果该帧上尚有旧页面，须从页表中删除映射
    if (frame->page_id_ != INVALID_PAGE_ID) {
      page_table_.erase(frame->page_id_);
      frame->page_id_ = INVALID_PAGE_ID;  // 重置页ID
    }
    // 重置帧状态（数据区、pin_count、dirty 等）
    frame->Reset();
  }

  // 3. 生成新 page id（原子自增，使用 relaxed 内存序）
  const page_id_t new_page_id = next_page_id_.fetch_add(1, std::memory_order_relaxed);

  // 4. 绑定新页与帧，并设置帧元数据
  auto &new_frame = frames_[frame_id];
  std::lock_guard<std::shared_mutex> new_write_lk(new_frame->rwlatch_);
  new_frame->pin_count_ = 0;  // 新页初始未被 pin
  new_frame->is_dirty_ = false;
  new_frame->page_id_ = new_page_id;
  page_table_[new_page_id] = frame_id;  // 更新页表映射

  // 5. 将新帧加入替换器
  // replacer_->RecordAccess(frame_id);
  // replacer_->SetEvictable(frame_id, false);

  return new_page_id;
}

/**
 * @brief 删除一个页（从磁盘和缓冲池中移除）
 *
 * 说明：如果页面在缓冲池中且被 pin（引用计数 > 0），不能删除，返回 false。
 *       否则要从页表中移除映射、将帧加入 free_frames_、并调用磁盘调度器释放磁盘上的页（DeallocatePage）。
 *
 * @param page_id 要删除的页 id
 * @return 若页面不存在或删除成功返回 true；若页面存在但无法删除（例如被 pin）返回 false
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // LOG_DEBUG("删除指定页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 检查页表中是否存在该页面
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 页面不在缓冲池中，认为删除成功
    return true;
  }

  // 获取页面所在的帧
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 若页面被 pin，不能删除
  if (frame->pin_count_ > 0) {
    return false;
  }

  // 页面未被锁定，可以删除
  // 先写回脏页（如果有）——这里先释放全局锁再调用 FlushPage 以避免长时间持有全局锁
  lk.unlock();
  // 如果帧是脏页，调用 FlushPage 将其安全写回磁盘
  if (frame->is_dirty_) {
    FlushPage(page_id);
  }
  // 重新上锁以修改全局结构
  lk.lock();

  // 复查页表，确保页面映射仍然有效且没有被其他线程改变
  it = page_table_.find(page_id);
  if (it == page_table_.end() || it->second != frame_id) {
    return true;
  }
  if (frame->pin_count_ > 0) {
    return false;
  }

  // 从页表中删除映射
  page_table_.erase(it);
  // 将帧标记为空闲加入 free_frames_
  free_frames_.push_back(frame_id);
  // 告诉替换器该帧不可被淘汰（已空闲）
  replacer_->SetEvictable(frame_id, false);
  // 通知磁盘调度器释放磁盘上的页空间
  disk_scheduler_->DeallocatePage(page_id);
  // 重置帧状态
  frame->Reset();

  return true;
}

/**
 * @brief 获取一个写保护的页守护对象（WritePageGuard）以进行可变访问
 *
 * 核心流程：
 *  1. 若页面已在缓冲池中，更新替换器访问信息，增加 pin_count 并返回写守护
 *  2. 否则，选择一个可用帧（free_frames_ 或通过替换器淘汰）
 *  3. 若淘汰帧为脏页则写回磁盘
 *  4. 从磁盘读取目标页面到选定帧
 *  5. 更新元数据并返回写守护
 *
 * 注意：若内存耗尽且无法淘汰，返回 std::nullopt
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // LOG_DEBUG("获取受保护的写页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 1. 检查页面是否已在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 页面在内存中，直接返回对应的写守护
    frame_id_t frame_id = it->second;
    auto &frame = frames_[frame_id];

    // 告诉替换器我们刚刚访问了该帧（用于 LRU-K 算法统计）并将其标记为不可淘汰
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    frame->pin_count_++;

    lk.unlock();
    return std::make_optional(WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // 2. 页面不在内存中，尝试分配或淘汰帧
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto maybe_evict_id = replacer_->Evict();
    if (!maybe_evict_id.has_value()) {
      // 无可淘汰帧，返回空
      return std::nullopt;
    }
    frame_id = maybe_evict_id.value();
    auto &evict_frame = frames_[frame_id];

    // 处理被淘汰帧的脏页写回
    std::lock_guard<std::shared_mutex> write_lk(evict_frame->rwlatch_);
    if (evict_frame->is_dirty_) {
      std::promise<bool> write_promise;
      auto write_future = write_promise.get_future();
      disk_scheduler_->Schedule({true, evict_frame->GetDataMut(), evict_frame->page_id_, std::move(write_promise)});
      if (!write_future.get()) {
        LOG_ERROR("Disk write failed during page deletion");
        return std::nullopt;
      }
      evict_frame->is_dirty_ = false;
    }
    // 从页表中移除被淘汰页面的映射并重置帧
    page_table_.erase(evict_frame->page_id_);
    evict_frame->Reset();
    replacer_->SetEvictable(frame_id, false);
  }
  // 3. 从磁盘读取目标页面到分配到的帧中
  auto &target_frame = frames_[frame_id];
  // 获取帧的读写锁（这里用 unique_lock 表示在读入页面时独占该帧）
  std::unique_lock<std::shared_mutex> read_lk(target_frame->rwlatch_);
  std::promise<bool> read_promise;
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, target_frame->GetDataMut(), page_id, std::move(read_promise)});
  if (!read_future.get()) {
    // 读盘失败，释放刚才占用的帧资源
    LOG_ERROR("读盘失败");
    free_frames_.push_back(frame_id);
    replacer_->SetEvictable(frame_id, true);
    return std::nullopt;
  }
  // 4. 更新缓冲池元数据
  page_table_[page_id] = frame_id;
  target_frame->page_id_ = page_id;
  target_frame->pin_count_++;
  target_frame->is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  read_lk.unlock();
  lk.unlock();
  return std::make_optional(WritePageGuard(page_id, target_frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief 获取一个读保护的页守护对象（ReadPageGuard）以进行只读访问
 *
 * 与 CheckedWritePage 类似，但允许并发多个读者，返回 ReadPageGuard
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // LOG_DEBUG("获取受保护的读页");
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  // 1. 页面已在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    auto &frame = frames_[frame_id];

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    frame->pin_count_++;

    lk.unlock();
    return std::make_optional(ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // 2. 页面不在内存中，分配或淘汰帧
  frame_id_t frame_id;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto maybe_evict_id = replacer_->Evict();
    if (!maybe_evict_id.has_value()) {
      return std::nullopt;
    }

    frame_id = maybe_evict_id.value();
    auto &evict_frame = frames_[frame_id];

    // 处理脏页写回（在独占写锁保护下）
    {
      std::lock_guard<std::shared_mutex> write_lk(evict_frame->rwlatch_);

      if (evict_frame->is_dirty_) {
        std::promise<bool> write_promise;
        auto write_future = write_promise.get_future();
        disk_scheduler_->Schedule({true, evict_frame->GetDataMut(), evict_frame->page_id_, std::move(write_promise)});
        if (!write_future.get()) {
          LOG_ERROR("Disk write failed during page deletion");
          return std::nullopt;
        }
        evict_frame->is_dirty_ = false;
      }
    }

    page_table_.erase(evict_frame->page_id_);
    evict_frame->Reset();
    replacer_->SetEvictable(frame_id, false);
  }

  // 3. 从磁盘读取目标页面
  auto &target_frame = frames_[frame_id];
  std::unique_lock<std::shared_mutex> read_lk(target_frame->rwlatch_);
  std::promise<bool> read_promise;
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, target_frame->GetDataMut(), page_id, std::move(read_promise)});
  if (!read_future.get()) {
    // 读盘失败，释放资源
    LOG_ERROR("读盘失败");
    free_frames_.push_back(frame_id);
    replacer_->SetEvictable(frame_id, true);
    return std::nullopt;
  }

  // 4. 更新缓冲池元数据
  page_table_[page_id] = frame_id;
  target_frame->page_id_ = page_id;
  target_frame->pin_count_ = 1;
  target_frame->is_dirty_ = false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  read_lk.unlock();
  lk.unlock();
  return std::make_optional(ReadPageGuard(page_id, target_frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief CheckedWritePage 的封装：若 CheckedWritePage 返回空则直接中止（仅供测试/便利用）
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief CheckedReadPage 的封装：若 CheckedReadPage 返回空则直接中止（仅供测试/便利用）
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 非线程安全地将页面数据写回磁盘（不对单帧加锁）
 *
 * 注意：此函数不对帧上锁，只访问
 * page_table_（因此调用方需保证一致性）。如果页面为脏页，会向磁盘调度器提交写请求但不等待结果。
 *
 * @param page_id 要刷新的页 id
 * @return 若页不在缓冲池中返回 false，否则返回 true
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // LOG_DEBUG("不安全的刷新页");
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 若为脏页，清除脏标志并发起异步写盘（不等待结果）
  if (frame->is_dirty_) {
    frame->is_dirty_ = false;

    std::promise<bool> write_promise;
    disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
  }

  return true;
}

/**
 * @brief 线程安全地将页面写回磁盘（会加锁以保证一致性）
 *
 * 实现：先在全局锁上查找帧，然后对帧加写锁并使用 disk_scheduler_ 同步写回磁盘，等待完成后清除脏标志。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // LOG_DEBUG("刷新页");
  // 先获取全局锁，查找页面所在帧
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // LOG_DEBUG("Page %d not found in page table", page_id);
    return false;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  lk.unlock();
  // 注意：这里没有显式对 frame->rwlatch_ 加写锁（原代码注释掉了），而是直接通过 disk_scheduler 提交写请求并等待结果。

  // 创建写操作的 promise 和 future
  std::promise<bool> write_promise;
  auto write_future = write_promise.get_future();

  // 提交写操作到磁盘调度器
  try {
    disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
  } catch (const std::exception &e) {
    LOG_ERROR("Failed to schedule disk write for page %d: %s", page_id, e.what());
    return false;
  }

  // 等待写操作完成并更新脏标志
  try {
    if (write_future.get()) {
      frame->is_dirty_ = false;
      // LOG_DEBUG("Successfully flushed page %d to disk", page_id);
      return true;
    }
  } catch (const std::exception &e) {
    LOG_ERROR("Exception during disk write for page %d: %s", page_id, e.what());
  }

  LOG_ERROR("Failed to flush page %d to disk", page_id);
  return false;
}

/**
 * @brief 非线程安全地刷新缓冲池中所有脏页（不对页面加锁）
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // LOG_DEBUG("不安全刷新全部页");
  for (const auto &[page_id, frame_id] : page_table_) {
    FlushPageUnsafe(page_id);
    // auto &frame = frames_[frame_id];

    // if (frame->is_dirty_) {
    //   frame->is_dirty_ = false;

    //   std::promise<bool> write_promise;
    //   disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});
    // }
  }
}

/**
 * @brief 线程安全地刷新缓冲池中所有页（会对每帧发起同步写回）
 */
void BufferPoolManager::FlushAllPages() {
  // LOG_DEBUG("刷新全部页");
  //std::scoped_lock<std::mutex> lk(*bpm_latch_);

  for (const auto &[page_id, frame_id] : page_table_) {
    FlushPage(page_id);
    // auto &frame = frames_[frame_id];
    //  {
    //    // 这里原本可以对 frame->rwlatch_ 加写锁以保证一致性
    //    // std::unique_lock<std::shared_mutex> write_lock(frame->rwlatch_);

    //   std::promise<bool> write_promise;
    //   auto write_future = write_promise.get_future();
    //   disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(write_promise)});

    //   if (write_future.get()) {
    //     frame->is_dirty_ = false;
    //   } else {
    //     LOG_ERROR("Failed to flush page %d to disk", page_id);
    //   }
    // }
  }
}

/**
 * @brief 获取页面的引用计数（pin_count）
 *
 * 说明：该方法对线程安全进行了说明：page_table_ 的访问需要全局锁保护，但 pin_count_ 本身是原子变量
 *
 * @param page_id 要查询的页 id
 * @return 若页面存在返回 pin_count，否则返回 std::nullopt
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // LOG_DEBUG("获取引用计数");
  std::scoped_lock<std::mutex> lk(*bpm_latch_);

  // 页面是否存在于页表中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];

  // 直接读取原子 pin_count_
  return std::make_optional(static_cast<size_t>(frame->pin_count_.load()));
}

}  // namespace bustub
