package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A trigger implementation which actually never fires any of the observed windows.
 */
public final class NoopTrigger<W extends Window> implements Trigger<W> {

  private static final NoopTrigger INSTANCE = new NoopTrigger();

  @SuppressWarnings("unchecked")
  public static <W extends Window> NoopTrigger<W> get() {
    return (NoopTrigger) INSTANCE;
  }

  private NoopTrigger() {}

  @Override
  public boolean isStateful() {
    return false;
  }

  @Override
  public TriggerResult onElement(long time, W window, TriggerContext ctx) {
    return TriggerResult.NOOP;
  }

  @Override
  public TriggerResult onTimer(long time, W window, TriggerContext ctx) {
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
