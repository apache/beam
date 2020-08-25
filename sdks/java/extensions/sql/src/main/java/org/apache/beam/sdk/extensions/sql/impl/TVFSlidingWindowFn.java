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
package org.apache.beam.sdk.extensions.sql.impl;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.impl.utils.TVFStreamingUtils;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/**
 * TVFSlidingWindowFn assigns window based on input row's "window_start" and "window_end"
 * timestamps.
 */
@AutoValue
public abstract class TVFSlidingWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {
  /** Size of the generated windows. */
  public abstract Duration getSize();

  /** Amount of time between generated windows. */
  public abstract Duration getPeriod();

  public static TVFSlidingWindowFn of(Duration size, Duration period) {
    return new AutoValue_TVFSlidingWindowFn(size, period);
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
    Row curRow = (Row) c.element();
    // In sliding window as TVF syntax, each row contains's its window's start and end as metadata,
    // thus we can assign a window directly based on window's start and end metadata.
    return Arrays.asList(
        new IntervalWindow(
            curRow.getDateTime(TVFStreamingUtils.WINDOW_START).toInstant(),
            curRow.getDateTime(TVFStreamingUtils.WINDOW_END).toInstant()));
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return equals(other);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    throw new UnsupportedOperationException(
        "TVFSlidingWindow does not support side input windows.");
  }
}
