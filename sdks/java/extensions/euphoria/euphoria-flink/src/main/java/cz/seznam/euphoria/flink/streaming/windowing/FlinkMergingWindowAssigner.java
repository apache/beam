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

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class FlinkMergingWindowAssigner<T, WID extends Window & Comparable<WID>>
  extends MergingWindowAssigner<MultiWindowedElement<WID, T>, FlinkWindow<WID>>
{
  private final FlinkWindowAssigner<T, WID> wrap;

  public FlinkMergingWindowAssigner(MergingWindowing<T, WID> windowing) {
    this.wrap = new FlinkWindowAssigner<>(windowing);
  }

  @Override
  public void mergeWindows(Collection<FlinkWindow<WID>> windows,
                           MergeCallback<FlinkWindow<WID>> callback) {
    @SuppressWarnings("unchecked")
    Collection<Pair<Collection<WID>, WID>> ms =
        ((MergingWindowing) this.wrap.getWindowing()).mergeWindows(
            windows.stream().map(FlinkWindow::getWindowID).collect(toSet()));
    for (Pair<Collection<WID>, WID> m : ms) {
      callback.merge(
          m.getFirst().stream().map(FlinkWindow::new).collect(toList()),
          new FlinkWindow<>(m.getSecond()));
    }
  }

  @Override
  public Collection<FlinkWindow<WID>> assignWindows(
      MultiWindowedElement<WID, T> element,
      long timestamp,
      WindowAssignerContext context) {

    return this.wrap.assignWindows(element, timestamp, context);
  }

  @Override
  public Trigger<MultiWindowedElement<WID, T>, FlinkWindow<WID>> getDefaultTrigger(
      StreamExecutionEnvironment env) {
    return this.wrap.getDefaultTrigger(env);
  }

  @Override
  public TypeSerializer<FlinkWindow<WID>> getWindowSerializer(ExecutionConfig executionConfig) {
    return this.wrap.getWindowSerializer(executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return this.wrap.isEventTime();
  }
}
