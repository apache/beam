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
package org.apache.beam.runners.spark.translation;

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window.Assign;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.Function;
import org.joda.time.Instant;

/** An implementation of {@link Assign} for the Spark runner. */
public class SparkAssignWindowFn<T, W extends BoundedWindow>
    implements Function<WindowedValue<T>, WindowedValue<T>> {

  private final WindowFn<? super T, W> fn;

  public SparkAssignWindowFn(WindowFn<? super T, W> fn) {
    this.fn = fn;
  }

  @Override
  @SuppressWarnings("unchecked")
  public WindowedValue<T> call(WindowedValue<T> windowedValue) throws Exception {
    final BoundedWindow boundedWindow = Iterables.getOnlyElement(windowedValue.getWindows());
    final T element = windowedValue.getValue();
    final Instant timestamp = windowedValue.getTimestamp();
    Collection<W> windows =
        ((WindowFn<T, W>) fn)
            .assignWindows(
                ((WindowFn<T, W>) fn).new AssignContext() {
                  @Override
                  public T element() {
                    return element;
                  }

                  @Override
                  public Instant timestamp() {
                    return timestamp;
                  }

                  @Override
                  public BoundedWindow window() {
                    return boundedWindow;
                  }
                });
    return WindowedValue.of(element, timestamp, windows, PaneInfo.NO_FIRING);
  }
}
