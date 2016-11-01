package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

import java.util.List;

/**
 * Composite {@link Trigger} that fires once after at least one of sub-triggers
 * have fired. In other words sub-triggers are composed using logical OR.
 */
public class AfterFirstCompositeTrigger<W extends Window> implements Trigger<W> {

  private final List<Trigger<W>> subtriggers;

  public AfterFirstCompositeTrigger(List<Trigger<W>> triggers) {
    this.subtriggers = triggers;
  }

  @Override
  public boolean isStateful() {
    for (Trigger<W> t : subtriggers) {
      if (t.isStateful()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TriggerResult onElement(long time, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onElement(time, window, ctx));
    }

    return r;
  }

  @Override
  public TriggerResult onTimer(long time, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onTimer(time, window, ctx));
    }

    return r;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {
    for (Trigger<W> t : subtriggers) {
      t.onClear(window, ctx);
    }
  }

  @Override
  public TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    TriggerResult r = TriggerResult.NOOP;
    for (Trigger<W> t : subtriggers) {
      r = TriggerResult.merge(
              r, t.onMerge(window, ctx));
    }

    return r;
  }
}
