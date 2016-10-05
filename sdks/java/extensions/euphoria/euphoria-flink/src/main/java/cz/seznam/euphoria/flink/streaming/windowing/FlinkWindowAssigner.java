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

public class FlinkWindowAssigner<T, LABEL>
    extends WindowAssigner<MultiWindowedElement<LABEL, T>, FlinkWindow<LABEL>> {

  private final Windowing<?, LABEL, ? extends WindowContext<LABEL>> windowing;
  private final ExecutionConfig cfg;

  public FlinkWindowAssigner(
      Windowing<?, LABEL, ? extends WindowContext<LABEL>> windowing,
      ExecutionConfig cfg) {
    
    this.windowing = windowing;
    this.cfg = cfg;
  }

  @Override
  public Collection<FlinkWindow<LABEL>> assignWindows(
      MultiWindowedElement<LABEL, T> element,
      long timestamp, WindowAssignerContext context) {

    List<FlinkWindow<LABEL>> ws = new ArrayList<>(element.windows().size());
    for (WindowID<LABEL> window : element.windows()) {
      ws.add(new FlinkWindow<>(windowing.createWindowContext(window)));
    }
    return ws;
  }

  @Override
  public Trigger<MultiWindowedElement<LABEL, T>, FlinkWindow<LABEL>> getDefaultTrigger(
      StreamExecutionEnvironment env) {
    return new FlinkWindowTrigger<>(windowing, cfg);
  }

  @Override
  public TypeSerializer<FlinkWindow<LABEL>> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) FlinkWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}
