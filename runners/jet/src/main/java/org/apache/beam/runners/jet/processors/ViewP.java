/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jet.processors;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's side input producing
 * primitives. Collects all input {@link WindowedValue}s, groups them by windows and keys and when
 * input is complete emits them.
 */
public class ViewP extends AbstractProcessor {

  private final TimestampCombiner timestampCombiner;
  private final Coder inputCoder;
  private final Coder outputCoder;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final String ownerId; // do not remove, useful for debugging

  private Map<BoundedWindow, TimestampAndValues> values = new HashMap<>();
  private Traverser<byte[]> resultTraverser;

  private ViewP(
      Coder inputCoder, Coder outputCoder, WindowingStrategy windowingStrategy, String ownerId) {
    this.timestampCombiner = windowingStrategy.getTimestampCombiner();
    this.inputCoder = inputCoder;
    this.outputCoder =
        Utils.deriveIterableValueCoder((WindowedValue.FullWindowedValueCoder) outputCoder);
    this.ownerId = ownerId;
  }

  @Override
  protected boolean tryProcess(int ordinal, @Nonnull Object item) {
    WindowedValue<?> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
    for (BoundedWindow window : windowedValue.getWindows()) {
      values.merge(
          window,
          new TimestampAndValues(
              windowedValue.getPane(), windowedValue.getTimestamp(), windowedValue.getValue()),
          (o, n) -> o.merge(timestampCombiner, n));
    }

    return true;
  }

  @Override
  public boolean complete() {
    if (resultTraverser == null) {
      resultTraverser =
          Traversers.traverseStream(
              values.entrySet().stream()
                  .map(
                      e -> {
                        WindowedValue<?> outputValue =
                            WindowedValue.of(
                                e.getValue().values,
                                e.getValue().timestamp,
                                Collections.singleton(e.getKey()),
                                e.getValue().pane);
                        return Utils.encode(outputValue, outputCoder);
                      }));
    }
    return emitFromTraverser(resultTraverser);
  }

  public static SupplierEx<Processor> supplier(
      Coder inputCoder,
      Coder outputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      String ownerId) {
    return () -> new ViewP(inputCoder, outputCoder, windowingStrategy, ownerId);
  }

  private static class TimestampAndValues {
    private final List<Object> values = new ArrayList<>();
    private Instant timestamp;
    private PaneInfo pane;

    TimestampAndValues(PaneInfo pane, Instant timestamp, Object value) {
      this.pane = pane;
      this.timestamp = timestamp;
      this.values.add(value);
    }

    public Iterable<Object> getValues() {
      return values;
    }

    TimestampAndValues merge(TimestampCombiner timestampCombiner, TimestampAndValues other) {
      pane = other.pane;
      timestamp = timestampCombiner.combine(timestamp, other.timestamp);
      values.addAll(other.values);
      return this;
    }
  }
}
