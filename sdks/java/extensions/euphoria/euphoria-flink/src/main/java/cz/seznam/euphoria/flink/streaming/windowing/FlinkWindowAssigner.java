package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import java.util.Arrays;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;
import java.util.Objects;

public class FlinkWindowAssigner<T, IN, GROUP, LABEL>
        extends WindowAssigner<T, FlinkWindow> {

  private final WindowingMode mode;
  private final Windowing<IN, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing;
  private final UnaryFunction<T, WindowID<GROUP, LABEL>> assigner;

  public FlinkWindowAssigner(
      Windowing<IN, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing,
      UnaryFunction<T, WindowID<GROUP, LABEL>> assigner) {
    
    this.mode = WindowingMode.determine(windowing);
    this.windowing = windowing;
    this.assigner = Objects.requireNonNull(assigner);
  }

  @Override
  public Collection<FlinkWindow> assignWindows(T element, long timestamp,
                                          WindowAssignerContext context) {
    
    WindowID<GROUP, LABEL> wid = assigner.apply(element);
    return Arrays.asList(new FlinkWindow(windowing.createWindowContext(wid)));
  }

  @Override
  public Trigger<T, FlinkWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
    return new FlinkTrigger<>(mode);
  }

  @Override
  public TypeSerializer<FlinkWindow> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>(FlinkWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return mode == WindowingMode.EVENT;
  }
  
}
