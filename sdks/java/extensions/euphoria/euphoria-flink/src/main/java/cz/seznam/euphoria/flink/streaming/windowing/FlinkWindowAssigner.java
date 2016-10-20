package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;
import java.util.stream.Collectors;

public class FlinkWindowAssigner<T, WID extends Window>
    extends WindowAssigner<MultiWindowedElement<WID, T>, FlinkWindow<WID>> {

  private final Windowing<T, WID> windowing;

  public FlinkWindowAssigner(Windowing<T, WID> windowing) {
    this.windowing = windowing;
  }

  @Override
  public Collection<FlinkWindow<WID>> assignWindows(
      MultiWindowedElement<WID, T> element,
      long timestamp, WindowAssignerContext context) {

    // map collection of Euphoria WIDs to FlinkWindows
    return element.windows().stream().map(
            FlinkWindow::new).collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Trigger<MultiWindowedElement<WID, T>, FlinkWindow<WID>> getDefaultTrigger(
      StreamExecutionEnvironment env) {
    return new FlinkWindowTrigger(windowing.getTrigger());
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeSerializer<FlinkWindow<WID>> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) FlinkWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}
