package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;

import java.util.Collections;
import java.util.Set;

class AttachedWindowing implements
    Windowing<Object, Object, Object, AttachedWindowing.AttachedWindowContext> {

  static class AttachedWindowContext extends WindowContext<Object, Object> {

    AttachedWindowContext(Object group, Object label) {
      super(WindowID.unaligned(group, label));
    }
    
    AttachedWindowContext(WindowID<Object, Object> id) {
      super(id);
    }

    @Override
    public String toString() {
      return "AttachedWindowContext(" + getWindowID() + ")";
    }
    
  }

  static final AttachedWindowing INSTANCE = new AttachedWindowing();

  @Override
  @SuppressWarnings("unchecked")
  public Set<WindowID<Object, Object>> assignWindowsToElement(
      WindowedElement<?, ?, Object> input) {
    return Collections.singleton((WindowID) input.getWindowID());
  }


  @Override
  public AttachedWindowContext createWindowContext(WindowID<Object, Object> id) {
    return new AttachedWindowContext(id);
  }


  private AttachedWindowing() {}
  
}
