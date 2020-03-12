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
package org.apache.beam.sdk.util;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;

/**
 * A lock which can always be acquired. It should not be used when a proper lock is required, but it
 * is useful as a performance optimization when locking is not necessary but the code paths have to
 * be shared between the locking and the non-locking variant.
 */
public class NoopLock implements Lock, Serializable {

  private static NoopLock instance;

  public static NoopLock get() {
    if (instance == null) {
      instance = new NoopLock();
    }
    return instance;
  }

  private NoopLock() {}

  @Override
  public void lock() {}

  @Override
  public void lockInterruptibly() {}

  @Override
  public boolean tryLock() {
    return true;
  }

  @Override
  public boolean tryLock(long time, @Nonnull TimeUnit unit) {
    return true;
  }

  @Override
  public void unlock() {}

  @Nonnull
  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
