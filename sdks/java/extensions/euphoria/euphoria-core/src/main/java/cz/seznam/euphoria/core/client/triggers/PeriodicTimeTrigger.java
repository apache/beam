package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that is periodically fired based on given time interval.
 * Used to implement "early triggering" functionality.
 */
public class PeriodicTimeTrigger<T> implements Trigger<T, TimeInterval> {

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicTimeTrigger.class);

  /** Next fire stamp (when merging the lowest timestamp is taken) */
  private final ValueStorageDescriptor<Long> fireTimeDescriptor =
          ValueStorageDescriptor.of("fire-time", Long.class, Long.MAX_VALUE, Math::min);

  private final long interval;

  public PeriodicTimeTrigger(long interval) {
    this.interval = interval;
  }

  @Override
  public TriggerResult onElement(long time, T element, TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(fireTimeDescriptor);

    if (fireStamp.get() == Long.MAX_VALUE) {
      // register first timer
      long start = time - (time % interval);
      long nextFireTimestamp = start + interval;

      ctx.registerTimer(nextFireTimestamp, window);
      fireStamp.set(nextFireTimestamp);
    }

    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimeEvent(long time, TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(fireTimeDescriptor);

    if (fireStamp.get().equals(time)) {
      LOG.debug("Firing PeriodicTimeTrigger, time {}, window: {}", time, window);
      ctx.registerTimer(time + interval, window);
      fireStamp.set(time + interval);

      return TriggerResult.FLUSH;
    }

    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(TimeInterval window, TriggerContext ctx) {
    ValueStorage<Long> fireStamp = ctx.getValueStorage(fireTimeDescriptor);
    ctx.deleteTimer(fireStamp.get(), window);
    fireStamp.clear();
  }

  @Override
  public TriggerResult onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
    ctx.mergeStoredState(fireTimeDescriptor);
    return TriggerResult.NOOP;
  }
}
