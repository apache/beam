package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.util.Collections;
import java.util.Set;

class AttachedWindowing<T, W extends Window> implements Windowing<T, W> {

  static final AttachedWindowing INSTANCE = new AttachedWindowing();

  @Override
  @SuppressWarnings("unchecked")
  public Set<W> assignWindowsToElement(WindowedElement<?, T> input) {
    return Collections.singleton((W) input.getWindow());
  }

  @Override
  public Trigger<W> getTrigger() {
    return NoopTrigger.get();
  }
}
