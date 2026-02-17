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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

public class TriggeredScheduledExecutorService extends ThreadPoolExecutor
    implements ScheduledExecutorService {
  private final BlockingQueue<FakeScheduledFuture> futures = new LinkedBlockingQueue<>();

  public TriggeredScheduledExecutorService() {
    super(0, 100, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
  }

  public boolean unblockNextFuture() throws InterruptedException {
    @Nullable FakeScheduledFuture f = futures.take();
    if (f == null) {
      return false;
    }
    f.triggerRun();
    return true;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long l, TimeUnit timeUnit) {
    FakeScheduledFuture f =
        new FakeScheduledFuture(runnable, Duration.ofMillis(timeUnit.toMillis(l)));
    try {
      futures.put(f);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return f;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long l, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("not supported yet");
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable runnable, long l, long l1, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("not supported yet");
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable runnable, long l, long l1, TimeUnit timeUnit) {
    throw new UnsupportedOperationException("not supported yet");
  }

  private class FakeScheduledFuture implements ScheduledFuture<Void> {
    private final Runnable r;
    private final Duration delay;
    private transient boolean cancelled;
    private final CompletableFuture<Void> delegateFuture = new CompletableFuture<>();

    private FakeScheduledFuture(Runnable r, Duration delay) {
      this.r = r;
      this.delay = delay;
    }

    void triggerRun() {
      TriggeredScheduledExecutorService.this.execute(
          () -> {
            try {
              r.run();
              delegateFuture.complete(null);
            } catch (RuntimeException e) {
              delegateFuture.completeExceptionally(e);
            }
          });
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
      return 0;
    }

    @Override
    public boolean cancel(boolean b) {
      cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return delegateFuture.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      return delegateFuture.get();
    }

    @Override
    public Void get(long l, TimeUnit timeUnit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return delegateFuture.get(l, timeUnit);
    }
  }
}
