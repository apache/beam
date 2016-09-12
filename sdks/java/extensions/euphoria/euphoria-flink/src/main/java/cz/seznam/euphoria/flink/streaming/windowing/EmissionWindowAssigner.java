package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;
import java.util.stream.Collectors;

public class EmissionWindowAssigner<T, W extends Window>
    extends WindowAssigner<T, EmissionWindow<W>>
{
  private WindowAssigner<T, W> inner;

  public EmissionWindowAssigner(WindowAssigner<T, W> inner) {
    this.inner = inner;
  }

  @Override
  public Collection<EmissionWindow<W>> assignWindows(
      T element, long timestamp, WindowAssigner.WindowAssignerContext context)
  {
    return inner.assignWindows(element, timestamp, context)
        .stream()
        .map(EmissionWindow::new)
        .collect(Collectors.toList());
  }

  @Override
  public Trigger<T, EmissionWindow<W>> getDefaultTrigger(StreamExecutionEnvironment env) {
    return new EmissionWindowTrigger<>(inner.getDefaultTrigger(env));
  }

  @Override
  public TypeSerializer<EmissionWindow<W>>
  getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) EmissionWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return inner.isEventTime();
  }
}
