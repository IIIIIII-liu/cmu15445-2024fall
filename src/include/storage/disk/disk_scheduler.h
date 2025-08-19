//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once  // 防止头文件被多次包含的预处理指令

#include <future>    // NOLINT // 包含了 std::promise and std::future，用于处理异步操作的结果
#include <optional>  // 包含了 std::optional，用于表示一个可能存在也可能不存在的值
#include <thread>    // NOLINT // 包含了 std::thread，用于创建和管理线程

#include "common/channel.h"  // 引入自定义的线程安全通道（队列），用于在线程间传递数据
#include "storage/disk/disk_manager.h"  // 引入磁盘管理器，实际执行磁盘读写操作

namespace bustub {

/**
 * @brief 代表一个将由 DiskManager 执行的读或写请求。
 *
 * 这个结构体封装了执行一次磁盘I/O操作所需的所有信息。
 */
struct DiskRequest {
  /** 标志位，指示这个请求是写操作 (true) 还是读操作 (false)。 */
  bool is_write_;

  /**
   * 指向内存中的数据缓冲区的指针。
   * 对于写操作，这是要写入磁盘的数据的来源。
   * 对于读操作，这是从磁盘读取数据后要存入的目标位置。
   */
  char *data_;

  /** 要读或写的磁盘页面的ID。 */
  page_id_t page_id_;

  /**
   * 一个 std::promise 对象，用于通知请求的发起者操作是否已成功完成。
   * 当后台线程处理完这个请求后，它会通过这个 promise 设置一个布尔值 (true 代表成功)。
   * 请求的发起者可以通过与此 promise 关联的 std::future 来阻塞等待操作完成的结果。
   */
  std::promise<bool> callback_;
};

/**
 * @brief DiskScheduler (磁盘调度器) 负责调度磁盘的读写操作。
 *
 * 通过调用 DiskScheduler::Schedule() 并传入一个适当的 DiskRequest 对象来调度一个请求。
 * 调度器维护一个后台工作线程，该线程使用磁盘管理器来处理已调度的请求。
 * 后台线程在 DiskScheduler 的构造函数中被创建，并在其析构函数中被 join（等待其结束）。
 * 这样做的目的是将磁盘I/O操作异步化，避免调用者线程因等待慢速的磁盘操作而被阻塞。
 */
class DiskScheduler {
 public:
  /**
   * @brief DiskScheduler 的构造函数。
   * @param disk_manager 指向一个 DiskManager 实例的指针，用于执行实际的磁盘I/O。
   */
  explicit DiskScheduler(DiskManager *disk_manager);

  /**
   * @brief DiskScheduler 的析构函数。
   *
   * 负责清理资源，特别是向请求队列发送一个停止信号 (`std::nullopt`)，
   * 然后 join 后台工作线程，确保它在对象销毁前安全退出。
   */
  ~DiskScheduler();

  /**
   * @brief 调度一个磁盘请求。
   *
   * 这个方法会将传入的请求 `r` 放入请求队列 `request_queue_` 中，
   * 之后后台工作线程会从队列中取出并处理它。
   * @param r 要调度的磁盘请求对象。
   */
  void Schedule(DiskRequest r);

  /**
   * @brief 启动后台工作线程。
   *
   * 这个函数包含了后台线程的主循环逻辑：
   * 1. 不断地从 `request_queue_` 中尝试取出请求。
   * 2. 如果取出的请求是 `std::nullopt`，说明收到了停止信号，线程退出循环。
   * 3. 如果是有效的 `DiskRequest`，则根据 `is_write_` 标志调用 `disk_manager_` 的
   *    `WritePage` 或 `ReadPage` 方法。
   * 4. 操作完成后，通过请求中的 `callback_` promise 设置操作结果。
   */
  void StartWorkerThread();

  // 为 std::promise<bool> 创建一个类型别名，方便使用。
  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief 创建一个 Promise 对象。如果你想实现自己版本的 promise，你可以修改这个函数，
   *        以便我们的测试用例可以使用你的 promise 实现。
   *
   * @return std::promise<bool> 返回一个新的 promise 对象。
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief 在磁盘上释放一个页面。
   *
   * 注意：在使用此方法之前，你应该查看 `BufferPoolManager` 中 `DeletePage` 的文档。
   * 这个操作通常是与缓冲池管理器协同完成的。
   *
   * @param page_id 要从磁盘上释放的页面的ID。
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** 指向磁盘管理器的指针。`__attribute__((__unused__))`
   * 告诉编译器这个变量可能不会被直接使用（在某些配置下），以避免警告。*/
  DiskManager *disk_manager_ __attribute__((__unused__));

  /**
   * 一个线程安全的队列，用于并发地调度和处理请求。
   * 当 DiskScheduler 的析构函数被调用时，会向队列中放入 `std::nullopt`，
   * 以此作为信号，通知后台线程停止执行。
   */
  Channel<std::optional<DiskRequest>> request_queue_;

  /**
   * 后台工作线程。它负责从 `request_queue_` 中取出请求，并交由磁盘管理器执行。
   * 使用 `std::optional` 包装，允许线程对象延迟创建或在某些情况下不存在。
   */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
