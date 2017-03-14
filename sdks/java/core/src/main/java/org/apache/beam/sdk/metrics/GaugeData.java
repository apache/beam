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
import java.io.Serializable;
import org.joda.time.Instant;

/**
 * Data describing the gauge. This should retain enough detail that it can be combined with
 * other {@link GaugeData}.
 */
@AutoValue
public abstract class GaugeData implements Serializable {

  public abstract long value();

  public abstract Instant timestamp();

  public static GaugeData create(long value) {
    return new AutoValue_GaugeData(value, Instant.now());
  }

  public static GaugeData empty() {
    return EmptyGaugeData.INSTANCE;
  }

  public GaugeData combine(GaugeData other) {
    if (this.timestamp().isAfter(other.timestamp())) {
      return this;
    } else {
      return other;
    }
  }

  public GaugeResult extractResult() {
    return GaugeResult.create(value(), timestamp());
  }

  /**
   * Empty {@link GaugeData}, representing no values reported.
   */
  public static class EmptyGaugeData extends GaugeData {

    private static final EmptyGaugeData INSTANCE = new EmptyGaugeData();
    private static final Instant EPOCH = new Instant(0);

    private EmptyGaugeData() {
    }

    @Override
    public long value() {
      return -1L;
    }

    @Override
    public Instant timestamp() {
      return EPOCH;
    }

    @Override
    public GaugeResult extractResult() {
      return GaugeResult.empty();
    }
  }
}
