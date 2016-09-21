package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Arrays;
import java.util.Collection;

public class FlinkWindowAssigner<T, IN, GROUP, LABEL>
        extends WindowAssigner<StreamingWindowedElement<?, ?, T>, FlinkWindowID> {

  private final Windowing<IN, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing;
  private final ExecutionConfig cfg;

  public FlinkWindowAssigner(
      Windowing<IN, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing,
      ExecutionConfig cfg) {
    
    this.windowing = windowing;
    this.cfg = cfg;
  }

  @Override
  public Collection<FlinkWindowID> assignWindows(
      StreamingWindowedElement<?, ?, T> element,
      long timestamp, WindowAssignerContext context) {
    
    WindowID<GROUP, LABEL> wid = (WindowID) element.getWindowID();
    return Arrays.asList(new FlinkWindowID(windowing.createWindowContext(wid)));
  }

  @Override
  public Trigger<StreamingWindowedElement<?, ?, T>, FlinkWindowID> getDefaultTrigger(
      StreamExecutionEnvironment env) {
    return new FlinkTrigger<>(windowing, cfg);
  }

  @Override
  public TypeSerializer<FlinkWindowID> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>(FlinkWindowID.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}
