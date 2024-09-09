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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@SuppressWarnings("initialization.fields.uninitialized")
@RunWith(JUnit4.class)
public class SubscriptionPartitionLoaderTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Mock SerializableFunction<TopicPath, Integer> getPartitionCount;

  @Mock SerializableSupplier<Boolean> terminate;
  private SubscriptionPartitionLoader loader;

  @Before
  public void setUp() {
    initMocks(this);
    FakeSerializable.Handle<SerializableFunction<TopicPath, Integer>> handle =
        FakeSerializable.put(getPartitionCount);
    FakeSerializable.Handle<SerializableSupplier<Boolean>> terminateHandle =
        FakeSerializable.put(terminate);
    loader =
        new SubscriptionPartitionLoader(
            example(TopicPath.class),
            example(SubscriptionPath.class),
            topic -> handle.get().apply(topic),
            Duration.millis(50),
            () -> terminateHandle.get().get());
  }

  @Test
  public void singleResult() {
    when(getPartitionCount.apply(example(TopicPath.class))).thenReturn(3);
    when(terminate.get()).thenReturn(false).thenReturn(false).thenReturn(true);
    PCollection<SubscriptionPartition> output = pipeline.apply(loader);
    PAssert.that(output)
        .containsInAnyOrder(
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(0)),
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(1)),
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(2)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void addedResults() {
    when(getPartitionCount.apply(example(TopicPath.class))).thenReturn(3).thenReturn(4);
    when(terminate.get()).thenReturn(false).thenReturn(false).thenReturn(true);
    PCollection<SubscriptionPartition> output = pipeline.apply(loader);
    PAssert.that(output)
        .containsInAnyOrder(
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(0)),
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(1)),
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(2)),
            SubscriptionPartition.of(example(SubscriptionPath.class), Partition.of(3)));
    pipeline.run().waitUntilFinish();
  }
}
