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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import com.google.auto.value.AutoValue;
import java.util.Comparator;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

@AutoValue
abstract class TimestampedValueWithId<T> {
  public static final Comparator<TimestampedValueWithId<?>> COMPARATOR =
      Comparator.<TimestampedValueWithId<?>, Instant>comparing(v -> v.getValue().getTimestamp())
          .thenComparingLong(TimestampedValueWithId::getId);

  static <T> TimestampedValueWithId<T> of(TimestampedValue<T> value, long id) {
    return new AutoValue_TimestampedValueWithId<>(value, id);
  }

  static <T> TimestampedValueWithId<T> bound(Instant ts) {
    return of(TimestampedValue.of(null, ts), Long.MIN_VALUE);
  }

  abstract TimestampedValue<T> getValue();

  abstract long getId();
}
