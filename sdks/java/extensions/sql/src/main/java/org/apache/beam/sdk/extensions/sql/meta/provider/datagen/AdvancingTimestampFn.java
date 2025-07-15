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
package org.apache.beam.sdk.extensions.sql.meta.provider.datagen;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

class AdvancingTimestampFn implements SerializableFunction<Long, Instant> {
  private final long maxOutOfOrdernessMs;
  private final Instant baseTime = Instant.now();

  AdvancingTimestampFn(long maxOutOfOrdernessMs) {
    this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
  }

  @Override
  public Instant apply(Long index) {
    long delay = (long) (ThreadLocalRandom.current().nextDouble() * maxOutOfOrdernessMs);
    return baseTime.plus(Duration.millis(index * 1000)).minus(Duration.millis(delay));
  }
}
