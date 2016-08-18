package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.executor.TriggerScheduler;
import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.Windowing;

import java.util.Collections;
import java.util.Set;

class DatumAttachedWindowing
    implements Windowing<Datum, Object,
                         Object, DatumAttachedWindowing.AttachedWindowContext>
{

  static class AttachedWindowContext extends WindowContext<Object, Object> {

    AttachedWindowContext(Object group, Object label) {
      super(WindowID.unaligned(group, label));
    }
    AttachedWindowContext(WindowID<Object, Object> id) {
      super(id);
    }
    
  }

  static final DatumAttachedWindowing INSTANCE = new DatumAttachedWindowing();

  @Override
  public Set<WindowID<Object, Object>> assignWindows(Datum input) {
    return Collections.singleton(WindowID.unaligned(input.group, input.label));
  }

  @Override
  public void updateTriggering(TriggerScheduler triggering, Datum input) {

  }

  @Override
  public AttachedWindowContext createWindowContext(WindowID<Object, Object> id) {
    return new AttachedWindowContext(id);
  }


  private DatumAttachedWindowing() {}
}
