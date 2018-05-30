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
package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.joda.time.Instant;

/**
 * The result of a {@link Gauge} metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class GaugeResult {
  public abstract long getValue();

  public abstract Instant getTimestamp();

  public static GaugeResult create(long value, Instant timestamp) {
    return new AutoValue_GaugeResult(value, timestamp);
  }

  public static GaugeResult empty() {
    return EmptyGaugeResult.INSTANCE;
  }

  /**
   * Empty {@link GaugeResult}, representing no values reported.
   */
  public static class EmptyGaugeResult extends GaugeResult {

    private static final EmptyGaugeResult INSTANCE = new EmptyGaugeResult();
    private static final Instant EPOCH = new Instant(0);

    private EmptyGaugeResult() {
    }

    @Override
    public long getValue() {
      return -1L;
    }

    @Override
    public Instant getTimestamp() {
      return EPOCH;
    }
  }
}
