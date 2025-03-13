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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.util;

import com.google.cloud.Timestamp;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import java.util.concurrent.TimeUnit;

public class TimestampGenerator extends Generator<Timestamp> {
  private static final long MIN_SECONDS = Timestamp.MIN_VALUE.getSeconds();
  private static final long MAX_SECONDS = Timestamp.MAX_VALUE.getSeconds();
  private static final int NANOS_PER_SECOND = (int) TimeUnit.SECONDS.toNanos(1);

  public TimestampGenerator() {
    super(Timestamp.class);
  }

  @Override
  public Timestamp generate(SourceOfRandomness random, GenerationStatus status) {
    final long seconds = random.nextLong(MIN_SECONDS, MAX_SECONDS);
    final int nanos = random.nextInt(0, NANOS_PER_SECOND);

    return Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
  }
}
