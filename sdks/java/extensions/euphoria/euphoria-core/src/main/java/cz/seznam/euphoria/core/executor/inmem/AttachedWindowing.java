package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.util.Collections;
import java.util.Set;

class AttachedWindowing<T, W extends Window> implements Windowing<T, W> {

  static final AttachedWindowing INSTANCE = new AttachedWindowing();

  @Override
  public Set<W> assignWindowsToElement(WindowedElement<?, T> input) {
    return Collections.singleton((W) input.getWindow());
  }

  @Override
  public Trigger<T, W> getTrigger() {
    return null;
  }

  private AttachedWindowing() {}
  
}
