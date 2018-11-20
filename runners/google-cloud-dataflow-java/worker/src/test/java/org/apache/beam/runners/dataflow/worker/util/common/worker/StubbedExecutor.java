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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.FixedClock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for testing single scheduled tasks.
 *
 * <p>The intended target is a task that is scheduled with {@link ScheduledExecutorService} and
 * during its execution schedules at most one more task; no other tasks are scheduled. This class
 * then provides a {@link ScheduledExecutorService} that allows manual control over running the
 * tasks. The client should instantiate this class with a clock, then use {@link getExecutor()} to
 * get the executor and use it to schedule the initial task, then use {@link runNextRunnable()} to
 * run the initial task, then use {@link runNextRunnable()} to run the second task, etc.
 */
public class StubbedExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(StubbedExecutor.class);

  private final FixedClock clock;
  @Mock private ScheduledExecutorService executor;
  private Runnable lastRunnable;
  private long lastDelay;

  public StubbedExecutor(FixedClock c) {
    this.clock = c;
    MockitoAnnotations.initMocks(this);
    when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> {
              assertNull(lastRunnable);
              Object[] args = invocation.getArguments();
              lastRunnable = (Runnable) args[0];
              lastDelay = (long) args[1];
              LOG.debug("{}: Saving task for later.", clock.currentTimeMillis());
              return null;
            });
  }

  public void runNextRunnable() {
    assertNotNull(lastRunnable);
    LOG.debug("{}: Running task after sleeping {} ms.", clock.currentTimeMillis(), lastDelay);
    clock.setTime(clock.currentTimeMillis() + lastDelay);
    Runnable r = lastRunnable;
    lastRunnable = null;
    r.run();
  }

  public ScheduledExecutorService getExecutor() {
    return executor;
  }
}
