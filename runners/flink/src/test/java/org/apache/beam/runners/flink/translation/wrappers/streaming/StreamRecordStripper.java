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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

class StreamRecordStripper {

  @SuppressWarnings("Guava")
  static <T> Iterable<WindowedValue<T>> stripStreamRecordFromWindowedValue(Iterable<Object> input) {
    return FluentIterable.from(input)
        .filter(
            o ->
                o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof WindowedValue)
        .transform(
            new Function<Object, WindowedValue<T>>() {
              @Nullable
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public WindowedValue<T> apply(@Nullable Object o) {
                if (o instanceof StreamRecord
                    && ((StreamRecord) o).getValue() instanceof WindowedValue) {
                  return (WindowedValue) ((StreamRecord) o).getValue();
                }
                throw new RuntimeException("unreachable");
              }
            });
  }
}
