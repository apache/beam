package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A trigger implementation which actually never fires any of the observed windows.
 */
public final class NoopTrigger<T, W extends Window> implements Trigger<T, W> {

  private static final NoopTrigger INSTANCE = new NoopTrigger();

  @SuppressWarnings("unchecked")
  public static <T, W extends Window> NoopTrigger<T, W> get() {
    return (NoopTrigger) INSTANCE;
  }

  private NoopTrigger() {}

  @Override
  public TriggerResult onElement(long time, T element, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimeEvent(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {

  }

  @Override
  public TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    return TriggerResult.NOOP;
  }
}
