package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerScheduler;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;

import java.util.Collections;
import java.util.List;
import java.util.Set;

class DatumAttachedWindowing
    implements Windowing<Datum, Object,
                         Object, DatumAttachedWindowing.AttachedWindow>
{
  static class AttachedWindow implements Window<Object, Object> {
    private final Object group;
    private final Object label;

    AttachedWindow(Object group, Object label) {
      this.group = group;
      this.label = label;
    }

    @Override
    public Object getGroup() {
      return group;
    }

    @Override
    public Object getLabel() {
      return label;
    }

    @Override
    public List<Trigger> createTriggers() {
      return Collections.emptyList();
    }
  }

  static final DatumAttachedWindowing INSTANCE = new DatumAttachedWindowing();

  @Override
  public Set<AttachedWindow> assignWindows(Datum input) {
    return Collections.singleton(new AttachedWindow(input.group, input.label));
  }

  @Override
  public void updateTriggering(TriggerScheduler triggering, Datum input) {

  }

  private DatumAttachedWindowing() {}
}
