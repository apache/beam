package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FlinkWindowAssigner<T, GROUP, LABEL>
    extends WindowAssigner<MultiWindowedElement<GROUP, LABEL, T>, FlinkWindow<GROUP, LABEL>> {

  private final Windowing<?, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing;
  private final ExecutionConfig cfg;

  public FlinkWindowAssigner(
      Windowing<?, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing,
      ExecutionConfig cfg) {
    
    this.windowing = windowing;
    this.cfg = cfg;
  }

  @Override
  public Collection<FlinkWindow<GROUP, LABEL>> assignWindows(
      MultiWindowedElement<GROUP, LABEL, T> element,
      long timestamp, WindowAssignerContext context) {

    List<FlinkWindow<GROUP, LABEL>> ws = new ArrayList<>(element.windows().size());
    for (WindowID<GROUP, LABEL> window : element.windows()) {
      ws.add(new FlinkWindow<>(windowing.createWindowContext(window)));
    }
    return ws;
  }

  @Override
  public Trigger<MultiWindowedElement<GROUP, LABEL, T>, FlinkWindow<GROUP, LABEL>> getDefaultTrigger(
      StreamExecutionEnvironment env) {
    return new FlinkWindowTrigger<>(windowing, cfg);
  }

  @Override
  public TypeSerializer<FlinkWindow<GROUP, LABEL>> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) FlinkWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}
