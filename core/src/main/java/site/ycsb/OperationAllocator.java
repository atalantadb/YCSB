/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 */
package site.ycsb;

import java.util.concurrent.atomic.AtomicLong;

/** Allocates a finite operation count in blocks shared by all client threads. */
final class OperationAllocator {
  private final AtomicLong remaining;
  private final int blockSize;

  OperationAllocator(long operationCount, int blockSize) {
    if (operationCount < 0) {
      throw new IllegalArgumentException("operationCount must not be negative");
    }
    if (blockSize <= 0) {
      throw new IllegalArgumentException("blockSize must be positive");
    }
    remaining = new AtomicLong(operationCount);
    this.blockSize = blockSize;
  }

  int claim() {
    while (true) {
      long current = remaining.get();
      if (current == 0) {
        return 0;
      }
      int claimed = (int) Math.min(current, blockSize);
      if (remaining.compareAndSet(current, current - claimed)) {
        return claimed;
      }
    }
  }

  void release(int count) {
    if (count > 0) {
      remaining.addAndGet(count);
    }
  }

  long remaining() {
    return remaining.get();
  }
}
