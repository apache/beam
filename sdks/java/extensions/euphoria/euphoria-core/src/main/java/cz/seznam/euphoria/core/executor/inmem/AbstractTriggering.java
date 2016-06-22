package cz.seznam.euphoria.core.executor.inmem;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.triggers.Triggerable;
import cz.seznam.euphoria.core.client.triggers.TriggerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Base class for various time triggering strategies
 */
public abstract class AbstractTriggering implements TriggerScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeTriggering.class);

  private transient ScheduledThreadPoolExecutor scheduler;
  private Multimap<Window, ScheduledTriggerTask> activeTasks =
          Multimaps.synchronizedMultimap(ArrayListMultimap.create());

  public AbstractTriggering() {
    this.initializeScheduler();
  }

  private void initializeScheduler() {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("trigger-%d").build();

    this.scheduler = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
    scheduler.setRemoveOnCancelPolicy(true);
  }

  @Override
  public ScheduledFuture<Void> scheduleAt(long stamp, Window w, Triggerable trigger) {
    long duration = stamp - getCurrentTimestamp();
    if (duration < 0) {
      return null;
    }
    return scheduleAfter(duration, w, new TriggerTask(stamp, w, trigger));
  }

  private ScheduledFuture<Void> scheduleAfter(long duration, Window w, TriggerTask task) {
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

  Multimap<Window, ScheduledTriggerTask> getScheduledTriggers() {
    return activeTasks;
  }

  @Override
  public void cancelAll() {
    activeTasks.values().stream().forEach(ScheduledTriggerTask::cancel);
    activeTasks.clear();
  }

  @Override
  public void cancel(Window w) {
    activeTasks.get(w).stream().forEach(ScheduledTriggerTask::cancel);
    activeTasks.asMap().remove(w);
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
    private final Window window;
    private final Triggerable trigger;

    public TriggerTask(long timestamp, Window w, Triggerable trigger) {
      this.timestamp = timestamp;
      this.window = w;
      this.trigger = trigger;
    }

    @Override
    public Void call() {
      try {
        trigger.fire(timestamp, window);
        AbstractTriggering.this.activeTasks.remove(window, this);
      } catch (Exception e) {
        LOG.error("Firing trigger " + trigger + " failed!", e);
      }

      return null;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Triggerable getTrigger() {
      return trigger;
    }

    public Window getWindow() {
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
