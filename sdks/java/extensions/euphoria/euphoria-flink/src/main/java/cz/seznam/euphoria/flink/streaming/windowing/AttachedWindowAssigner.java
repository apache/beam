/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;
import java.util.Collections;

public class AttachedWindowAssigner<WID extends Window, T>
    extends WindowAssigner<StreamingWindowedElement<WID, T>,
                           AttachedWindow<WID>>
{
  @Override
  public Collection<AttachedWindow<WID>>
  assignWindows(StreamingWindowedElement<WID, T> element,
                long timestamp,
                WindowAssignerContext context) {
    return Collections.singleton(new AttachedWindow<>(element));
  }

  @Override
  public Trigger<StreamingWindowedElement<WID, T>, AttachedWindow<WID>>
  getDefaultTrigger(StreamExecutionEnvironment env) {
    return new AttachedWindowTrigger<>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeSerializer<AttachedWindow<WID>> getWindowSerializer(ExecutionConfig executionConfig) {
    return new KryoSerializer<>((Class) AttachedWindow.class, executionConfig);
  }

  @Override
  public boolean isEventTime() {
    return true;
  }
}