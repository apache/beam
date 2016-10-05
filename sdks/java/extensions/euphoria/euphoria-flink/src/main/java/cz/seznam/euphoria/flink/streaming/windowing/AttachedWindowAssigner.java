package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;
import java.util.Collections;

public class AttachedWindowAssigner<LABEL, T>
    extends WindowAssigner<StreamingWindowedElement<LABEL, T>,
                           AttachedWindow<LABEL>>
{
  @Override
  public Collection<AttachedWindow<LABEL>>
  assignWindows(StreamingWindowedElement<LABEL, T> element,
                long timestamp,
                WindowAssignerContext context) {
    return Collections.singleton(new AttachedWindow<>(element));
  }

  @Override
  public Trigger<StreamingWindowedElement<LABEL, T>, AttachedWindow<LABEL>>
  getDefaultTrigger(StreamExecutionEnvironment env) {
    return new AttachedWindowTrigger<>();
  }

  @Override
  public TypeSerializer<AttachedWindow<LABEL>> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) AttachedWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}