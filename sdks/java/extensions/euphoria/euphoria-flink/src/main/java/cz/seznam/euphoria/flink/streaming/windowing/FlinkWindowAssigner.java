/**
 * Copyright 2016 Seznam a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  Windowing<T, WID> getWindowing() {
    return this.windowing;
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
