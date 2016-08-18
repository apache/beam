package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.WindowedElement;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.Windowing;

import java.util.Collections;
import java.util.Set;

class AttachedWindowing
    implements Windowing<WindowedElement, Object,
                         Object, AttachedWindowing.AttachedWindowContext>
{

  static class AttachedWindowContext extends WindowContext<Object, Object> {

    AttachedWindowContext(Object group, Object label) {
      super(WindowID.unaligned(group, label));
    }
    AttachedWindowContext(WindowID<Object, Object> id) {
      super(id);
    }
    
  }

  static final AttachedWindowing INSTANCE = new AttachedWindowing();

  @Override
  public Set<WindowID<Object, Object>> assignWindows(WindowedElement input) {
    return Collections.singleton(input.getWindowID());
  }

  @Override
  public void updateTriggering(TriggerScheduler triggering, WindowedElement input) {

  }

  @Override
  public AttachedWindowContext createWindowContext(WindowID<Object, Object> id) {
    return new AttachedWindowContext(id);
  }


  private AttachedWindowing() {}
}
