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
package org.apache.beam.sdk.io.solace.read;

import com.google.common.collect.Ordering;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkPolicy<T> implements Serializable {
  public static final Logger LOG = LoggerFactory.getLogger(WatermarkPolicy.class);
  private WatermarkParameters<T> watermarkParameters;

  public static <T> WatermarkPolicy<T> create(SerializableFunction<T, Instant> timestampFunction) {
    return new WatermarkPolicy<T>(WatermarkParameters.<T>create(timestampFunction));
  }

  private WatermarkPolicy(WatermarkParameters<T> watermarkParameters) {
    this.watermarkParameters = watermarkParameters;
  }

  public Instant getWatermark() {
    Instant now = Instant.now();
    Instant watermarkIdleThreshold =
        now.minus(watermarkParameters.getWatermarkIdleDurationThreshold());

    Instant newWatermark =
        watermarkParameters.getLastUpdateTime().isBefore(watermarkIdleThreshold)
            ? watermarkIdleThreshold
            : watermarkParameters.getLastSavedWatermark();

    if (newWatermark.isAfter(watermarkParameters.getCurrentWatermark())) {
      watermarkParameters =
          watermarkParameters.toBuilder().setCurrentWatermark(newWatermark).build();
    }
    return watermarkParameters.getCurrentWatermark();
  }

  public void update(T record) {
    watermarkParameters =
        watermarkParameters
            .toBuilder()
            .setLastSavedWatermark(
                Ordering.natural()
                    .max(
                        watermarkParameters.getLastSavedWatermark(),
                        watermarkParameters.getTimestampFn().apply(record)))
            .setLastUpdateTime(Instant.now())
            .build();
  }
}
