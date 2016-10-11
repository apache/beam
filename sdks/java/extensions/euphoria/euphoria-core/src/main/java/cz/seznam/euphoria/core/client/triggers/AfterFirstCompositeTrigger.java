package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.ArrayListMultimap;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Multimap;

import java.util.Arrays;
import java.util.List;

/**
 * Composite {@link Trigger} that fires once after at least one of sub-triggers
 * have fired. In other words sub-triggers are composed using logical OR.
 */
public class AfterFirstCompositeTrigger<T, W extends Window> implements Trigger<T, W> {

  private final List<Trigger<T, W>> subtriggers;

  public AfterFirstCompositeTrigger(List<Trigger<T, W>> triggers) {
    this.subtriggers = triggers;
  }

  @Override
  public TriggerResult onElement(long time, T element, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<T, W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onElement(time, element, window, ctx));
    }

    return r;
  }

  @Override
  public TriggerResult onTimeEvent(long time, W window, TriggerContext ctx) {
    TriggerResult r = TriggerResult.NOOP;

    // propagate event to all triggers
    for (Trigger<T, W> t : subtriggers) {
      r = TriggerResult.merge(r,
              t.onTimeEvent(time, window, ctx));
    }

    return r;
  }

  @Override
  public void onClear(W window, TriggerContext ctx) {
    for (Trigger<T, W> t : subtriggers) {
      t.onClear(window, ctx);
    }
  }

  @Override
  public TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx) {
    TriggerResult r = TriggerResult.NOOP;
    for (Trigger<T, W> t : subtriggers) {
      r = TriggerResult.merge(
              r, t.onMerge(window, ctx));
    }

    return r;
  }
}
