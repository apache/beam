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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Base class for various time triggering strategies. */
public abstract class AbstractTriggerScheduler implements TriggerScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTriggerScheduler.class);
  private final Multimap<KeyedWindow, ScheduledTriggerTask> activeTasks =
      Multimaps.synchronizedMultimap(ArrayListMultimap.create());
  private transient ScheduledThreadPoolExecutor scheduler;

  public AbstractTriggerScheduler() {
    this.initializeScheduler();
  }

  private void initializeScheduler() {
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("trigger-%d").setDaemon(true).build();

    this.scheduler = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
    scheduler.setRemoveOnCancelPolicy(true);
  }

  @Override
  public boolean scheduleAt(long stamp, KeyedWindow w, Triggerable trigger) {
    long currentStamp = getCurrentTimestamp();
    long duration = stamp - currentStamp;
    if (duration < 0) {
      return false;
    }
    scheduleAfter(duration, w, new TriggerTask(stamp, w, trigger));
    return true;
  }

  private ScheduledFuture<Void> scheduleAfter(long duration, KeyedWindow w, TriggerTask task) {
    ScheduledFuture<Void> future = scheduler.schedule(task, duration, TimeUnit.MILLISECONDS);
    activeTasks.put(w, new ScheduledTriggerTask(task, future));
    return future;
  }

  @Override
  public void close() {
    cancelAll();
    activeTasks.clear();
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @Override
  public void cancelAll() {
    synchronized (activeTasks) {
      for (ScheduledTriggerTask t : activeTasks.values()) {
        t.cancel();
      }
      activeTasks.clear();
    }
  }

  @Override
  public void cancel(long stamp, KeyedWindow w) {
    synchronized (activeTasks) {
      Collection<ScheduledTriggerTask> tasks = activeTasks.get(w);
      if (tasks != null && !tasks.isEmpty()) {
        List<ScheduledTriggerTask> canceled = new ArrayList<>();
        for (ScheduledTriggerTask task : tasks) {
          if (task.getTimestamp() == stamp) {
            task.cancel();
            canceled.add(task);
          }
        }
        tasks.removeAll(canceled);
        if (tasks.isEmpty()) {
          // ~ garbage collect
          activeTasks.removeAll(w);
        }
      }
    }
  }

  /** @return current timestamp determined according to chosen triggering strategy */
  @Override
  public abstract long getCurrentTimestamp();

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.initializeScheduler();
  }

  /** Trigger task to be scheduled. */
  class TriggerTask implements Callable<Void> {
    private final long timestamp;
    private final KeyedWindow window;
    private final Triggerable trigger;

    public TriggerTask(long timestamp, KeyedWindow w, Triggerable trigger) {
      this.timestamp = timestamp;
      this.window = w;
      this.trigger = trigger;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void call() {
      try {
        trigger.fire(timestamp, window);
        AbstractTriggerScheduler.this.activeTasks.remove(window, this);
      } catch (Exception e) {
        LOG.warn("Firing trigger " + trigger + " failed!", e);
      }
      return null;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Triggerable getTrigger() {
      return trigger;
    }

    public KeyedWindow getWindow() {
      return window;
    }
  }

  /** Trigger task in scheduled state that can be cancelled. */
  class ScheduledTriggerTask extends TriggerTask {
    private final ScheduledFuture<Void> future;

    public ScheduledTriggerTask(TriggerTask task, ScheduledFuture<Void> future) {
      super(task.timestamp, task.window, task.trigger);
      this.future = future;
    }

    public void cancel() {
      future.cancel(false);
    }
  }
}
