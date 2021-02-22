#pragma once
#include <atomic>
#include "serializer.h"

struct SSdbRow;
struct objectBase {
  int32_t              updateEnd{0};
  std::atomic<int32_t> refCount{0};
 public:
  virtual ~objectBase() {}
  virtual int32_t insert() = 0;
  virtual int32_t remove() = 0;
  virtual int32_t encode(SSdbRow *pRow) = 0;
  virtual int32_t update() = 0;
};