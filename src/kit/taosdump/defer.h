#pragma once
#include <iostream>
#include <utility>

// Deferred allows you to ensure something gets run at the end of a scope
template <class F>
class Deferred {
 public:
  explicit Deferred(F f) noexcept : fn(std::move(f)) {}
  Deferred(Deferred&& other) noexcept : fn(std::move(other.fn)), called(std::exchange(other.called, false)) {}
  Deferred(const Deferred&) = delete;
  Deferred& operator=(const Deferred&) = delete;
  Deferred& operator=(Deferred&&) = delete;

  ~Deferred() noexcept {
    if (called) fn();
  }
  void cancel() { called = false; }

 private:
  F    fn;
  bool called{true};
};

// defer() - convenience function to generate a Deferred
template <class F>
Deferred<F> defer(const F& f) noexcept {
  return Deferred<F>(f);
}

template <class F>
Deferred<F> defer(F&& f) noexcept {
  return Deferred<F>(std::forward<F>(f));
}