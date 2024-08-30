/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemoryLimiterImpl implements MemoryLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryLimiterImpl.class);
  private final long minBlockSize;
  private final long maxBlockSize;
  private final long maxAvailable;

  @GuardedBy("this")
  private long available;

  public MemoryLimiterImpl(long minBlockSize, long maxBlockSize, long maxAvailable) {
    this.minBlockSize = minBlockSize;
    this.maxBlockSize = maxBlockSize;
    this.maxAvailable = maxAvailable;
    this.available = maxAvailable;
  }

  @Override
  public synchronized Block claim(long toAcquire) {
    toAcquire = Math.max(Math.min(toAcquire, available / 2), minBlockSize);
    available -= toAcquire;
    return new Block(toAcquire);
  }

  @Override
  public long minBlockSize() {
    return minBlockSize;
  }

  @Override
  public long maxBlockSize() {
    return maxBlockSize;
  }

  private synchronized void release(long toRelease) {
    available += toRelease;
    checkState(available <= maxAvailable);
  }

  public class Block implements MemoryLimiter.Block {
    public final long claimed;
    private boolean released = false;

    private Block(long claimed) {
      this.claimed = claimed;
    }

    @Override
    public long claimed() {
      return claimed;
    }

    @Override
    public void close() {
      checkState(!released);
      released = true;
      release(claimed);
    }

    @Override
    public void finalize() {
      if (!released) {
        LOG.error("Failed to release memory block- likely SDF implementation error.");
        close();
      }
    }
  }
}
