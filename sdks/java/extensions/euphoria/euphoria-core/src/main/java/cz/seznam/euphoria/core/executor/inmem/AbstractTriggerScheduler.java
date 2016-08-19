package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.ArrayListMultimap;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Multimap;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Multimaps;
import cz.seznam.euphoria.guava.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.triggers.Triggerable;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
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

/**
 * Base class for various time triggering strategies
 */
public abstract class AbstractTriggerScheduler implements TriggerScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTriggerScheduler.class);

  private transient ScheduledThreadPoolExecutor scheduler;
  private final Multimap<WindowContext, ScheduledTriggerTask> activeTasks =
          Multimaps.synchronizedMultimap(ArrayListMultimap.create());

  public AbstractTriggerScheduler() {
    this.initializeScheduler();
  }

  private void initializeScheduler() {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("trigger-%d")
            .setDaemon(true)
            .build();

    this.scheduler = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
    scheduler.setRemoveOnCancelPolicy(true);
  }

  @Override
  public ScheduledFuture<Void> scheduleAt(long stamp, WindowContext w, Triggerable trigger) {
    long duration = stamp - getCurrentTimestamp();
    if (duration < 0) {
      return null;
    }
    return scheduleAfter(duration, w, new TriggerTask(stamp, w, trigger));
  }

  private ScheduledFuture<Void> scheduleAfter(long duration, WindowContext w, TriggerTask task) {
    ScheduledFuture<Void> future = scheduler.schedule(
            task,
            duration,
            TimeUnit.MILLISECONDS);
    activeTasks.put(w, new ScheduledTriggerTask(task, future));
    return future;
  }

  @Override
  public void close() {
    activeTasks.clear();
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @Override
  public void cancelAll() {
    cancelAllImpl();
  }

  final List<ScheduledTriggerTask> cancelAllImpl() {
    synchronized (activeTasks) {
      List<ScheduledTriggerTask> canceled = new ArrayList<>();
      for (ScheduledTriggerTask t : activeTasks.values()) {
        canceled.add(t);
        t.cancel();
      }
      activeTasks.clear();
      return canceled;
    }
  }

  @Override
  public void cancel(WindowContext w) {
    synchronized (activeTasks) {
      Collection<ScheduledTriggerTask> tasks = activeTasks.get(w);
      if (tasks != null && !tasks.isEmpty()) {
        tasks.stream().forEach(ScheduledTriggerTask::cancel);
        activeTasks.removeAll(w);
      }
    }
  }

  /**
   * @return current timestamp determined according to chosen triggering strategy
   */
  public abstract long getCurrentTimestamp();

  /**
   * Trigger task to be scheduled
   */
  class TriggerTask implements Callable<Void> {
    private final long timestamp;
    private final WindowContext window;
    private final Triggerable trigger;

    public TriggerTask(long timestamp, WindowContext w, Triggerable trigger) {
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

    public WindowContext getWindow() {
      return window;
    }
  }

  /**
   * Trigger task in scheduled state that can be cancelled
   */
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

  private void readObject(ObjectInputStream in)
          throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    this.initializeScheduler();
  }
}
