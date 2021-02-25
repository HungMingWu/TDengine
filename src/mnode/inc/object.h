#pragma once
#include <atomic>
#include <memory>
#include "serializer.h"

struct SSdbRow;
struct objectBase {
  int32_t              updateEnd{0};
  std::atomic<int32_t> refCount{0};
 public:
  virtual ~objectBase() {}
  virtual int32_t insert() = 0;
  virtual int32_t remove() = 0;
  virtual int32_t encode(binser::memory_output_archive<> &) = 0;
  virtual int32_t update() = 0;
};
using ObjectPtr = std::shared_ptr<objectBase>;