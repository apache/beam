package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

/**
 * A {@link Trigger} that fires once the count of elements reaches given count.
 */
public class CountTrigger<T, W extends Window> implements Trigger<T, W> {

  private final ValueStorageDescriptor<Long> countDesc =
          ValueStorageDescriptor.of("count", Long.class, 0L, (x, y) -> x + y );

  private final long maxCount;

  public CountTrigger(long maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public TriggerResult onElement(long time, T element, W window, TriggerContext ctx) {
    ValueStorage<Long> count = ctx.getValueStorage(countDesc);

    count.set(count.get() + 1L);

    if (count.get() >= maxCount) {
      count.clear();
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimeEvent(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {
    ctx.getValueStorage(countDesc).clear();
  }

  @Override
  public TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    ctx.mergeStoredState(countDesc);
    ValueStorage<Long> count = ctx.getValueStorage(countDesc);

    if (count.get() >= maxCount) {
      count.clear();
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }
}
