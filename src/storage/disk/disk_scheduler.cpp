//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * @brief Schedules a request for the DiskManager to execute.
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  // 检查传入的请求是否有效。如果无效，则通过 promise 异步地传递一个异常。
  if (r.data_ == nullptr || r.page_id_ == INVALID_PAGE_ID) {
    r.callback_.set_exception(std::make_exception_ptr(
        Exception(ExceptionType::INVALID, "Invalid request: data is null or page_id is invalid.")));
    return;  // 无效请求不入队，直接返回
  }
  // 将有效的请求移入队列。这里可以直接移动 r，Channel内部会负责包装成 optional。
  request_queue_.Put(std::optional<DiskRequest>(std::move(r)));
}

/**
 * @brief Background worker thread function that processes scheduled requests.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    // 阻塞式地从队列中获取请求
    auto request_opt = request_queue_.Get();

    // 如果获取到的是 nullopt (哨兵信号)，则退出循环，线程结束
    if (!request_opt.has_value()) {
      break;
    }

    // 从 optional 中移出请求对象，提高效率
    DiskRequest request = std::move(request_opt.value());

    try {
      // 根据请求类型执行读或写操作
      if (request.is_write_) {
        disk_manager_->WritePage(request.page_id_, request.data_);
      } else {
        disk_manager_->ReadPage(request.page_id_, request.data_);
      }
      // 操作成功，通过 promise 设置结果为 true
      request.callback_.set_value(true);
    } catch (...) {
      // 如果磁盘操作过程中发生任何异常，则捕获它
      //并通过 promise 将异常传递给调用者
      request.callback_.set_exception(std::current_exception());
    }
  }
}

}  // namespace bustub