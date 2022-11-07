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

import java.util.concurrent.atomic.AtomicLong;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * This object quickly moves time forward based upon how much it has been asked to sleep, without
 * actually sleeping, to simulate the backoff.
 */
public class FastNanoClockAndSleeper extends ExternalResource
    implements NanoClock, Sleeper, TestRule {
  private AtomicLong fastNanoTime = new AtomicLong();

  @Override
  public long nanoTime() {
    return fastNanoTime.get();
  }

  @Override
  protected void before() throws Throwable {
    fastNanoTime = new AtomicLong(NanoClock.SYSTEM.nanoTime());
  }

  @Override
  public void sleep(long millis) throws InterruptedException {
    fastNanoTime.addAndGet(millis * 1000000L);
  }
}
