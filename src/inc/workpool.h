#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

template <typename Item, typename derived>
class workpool {
  std::vector<std::thread> pools;
  std::atomic<bool>        stop_{false};
  std::mutex               mutex_;
  std::condition_variable  cv_;
  std::vector<Item>        items;
  void run() {
    while (!stop_) {
      std::unique_lock<std::mutex> l(mutex_);
      cv_.wait(l, [this] { return !items.empty() || stop_; });
      if (stop_) break;
      auto moveItem = std::move(items);
      l.unlock();
      process_(std::move(moveItem));
    }
  }
  void process_(std::vector<Item> item) {
      (static_cast<derived&>(*this)).process(std::move(item));
  }
 public:
  workpool(size_t thrds) {
    for (size_t i = 0; i < thrds; i++) pools.emplace_back([this] { run(); });
  }
  virtual ~workpool() {
    for (auto &t : pools) t.join();
  }
  void stop() {
    stop_ = true;
    cv_.notify_all();
  }
  void put(const Item& newItem) {
    std::lock_guard<std::mutex> l(mutex_);
    items.push_back(newItem);
    cv_.notify_one();
  }
};