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
package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Tests for {@link ThrottleWithoutExternalResource}. */
@RunWith(JUnit4.class)
public class ThrottleWithoutExternalResourceTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenNonSparseElements_thenAssignChannelsNominallyDistributed() {}

  @Test
  public void givenSparseElements_thenAssignChannelsNominallyDistributed() {}

  @Test
  public void givenLargeElementSize_thenThrowsWithSizeReport() {}

  @Test
  public void givenSparseElementPulse_thenEmitsAllImmediately() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    List<Integer> list = Stream.iterate(0, i->i+1).limit(3).collect(Collectors.toList());

    PCollection<Integer> throttled = pipeline
            .apply(Create.of(list))
            .apply(ThrottleWithoutExternalResource.of(ThrottleWithoutExternalResource.Configuration.builder()
                            .setMaximumRate(rate)
                    .build()));

    PAssert.that(throttled).containsInAnyOrder(list);

    pipeline.run();
  }

  @Test
  public void offsetRange_isSerializable() {
    SerializableUtils.ensureSerializable(ThrottleWithoutExternalResource.OffsetRange.empty());
  }

  @Test
  public void offsetRangeTracker_isSerializable() {
    SerializableUtils.ensureSerializable(ThrottleWithoutExternalResource.OffsetRange.empty().newTracker());
  }

  @Test
  public void configuration_isSerializable() {
    SerializableUtils.ensureSerializable(ThrottleWithoutExternalResource.Configuration.builder()
                    .setMaximumRate(Rate.of(1, Duration.ZERO))
            .build());
  }
}
