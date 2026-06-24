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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaTimestampType.LOG_APPEND_TIME;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.kafka.KafkaReadRedistribute.AssignOffsetShardFn;
import org.apache.beam.sdk.io.kafka.KafkaReadRedistribute.AssignRecordKeyFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KafkaReadRedistribute}. */
@RunWith(JUnit4.class)
public class KafkaReadRedistributeTest implements Serializable {

  private static final ImmutableList<KafkaRecord<String, Integer>> INPUTS =
      ImmutableList.of(
          makeKafkaRecord("k1", 3, 1),
          makeKafkaRecord("k5", Integer.MAX_VALUE, 2),
          makeKafkaRecord("k5", Integer.MIN_VALUE, 3),
          makeKafkaRecord("k2", 66, 4),
          makeKafkaRecord("k1", 4, 5),
          makeKafkaRecord("k2", -33, 6),
          makeKafkaRecord("k3", 0, 7));

  private static final ImmutableList<KafkaRecord<String, Integer>> SAME_OFFSET_INPUTS =
      ImmutableList.of(
          makeKafkaRecord("k1", 3, 1),
          makeKafkaRecord("k5", Integer.MAX_VALUE, 1),
          makeKafkaRecord("k5", Integer.MIN_VALUE, 1),
          makeKafkaRecord("k2", 66, 1),
          makeKafkaRecord("k1", 4, 1),
          makeKafkaRecord("k2", -33, 1),
          makeKafkaRecord("k3", 0, 1));

  private static final ImmutableList<KafkaRecord<String, Integer>> SAME_KEY_INPUTS =
      ImmutableList.of(
          makeKafkaRecord("k1", 3, 1),
          makeKafkaRecord("k1", Integer.MAX_VALUE, 2),
          makeKafkaRecord("k1", Integer.MIN_VALUE, 3),
          makeKafkaRecord("k1", 66, 4),
          makeKafkaRecord("k1", 4, 5),
          makeKafkaRecord("k1", -33, 6),
          makeKafkaRecord("k1", 0, 7));

  static KafkaRecord<String, Integer> makeKafkaRecord(String key, Integer value, Integer offset) {
    return new KafkaRecord<String, Integer>(
        /*topic*/ "kafka",
        /*partition*/ 1,
        /*offset*/ offset,
        /*timestamp*/ 123,
        /*timestampType*/ LOG_APPEND_TIME,
        /*headers*/ null,
        key,
        value);
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testRedistributeByOffsetShard() {

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(INPUTS)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KafkaRecord<String, Integer>> output =
        input.apply(KafkaReadRedistribute.byOffsetShard(/*numBuckets*/ 10));

    PAssert.that(output).containsInAnyOrder(INPUTS);

    assertEquals(input.getWindowingStrategy(), output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testRedistributeByKey() {

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(INPUTS)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KafkaRecord<String, Integer>> output =
        input.apply(KafkaReadRedistribute.byRecordKey(10));

    PAssert.that(output).containsInAnyOrder(INPUTS);

    assertEquals(input.getWindowingStrategy(), output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testAssignOutputShardFnBucketing() {
    List<KafkaRecord<String, Integer>> inputs = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      inputs.addAll(INPUTS);
    }

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(inputs)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<Integer> output =
        input
            .apply(ParDo.of(new AssignOffsetShardFn<String, Integer>(2)))
            .apply(GroupByKey.create())
            .apply(MapElements.into(integers()).via(KV::getKey));

    PAssert.that(output).containsInAnyOrder(ImmutableList.of(0, 1));

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testAssignRecordKeyFnBucketing() {
    List<KafkaRecord<String, Integer>> inputs = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      inputs.addAll(INPUTS);
    }

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(inputs)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<Integer> output =
        input
            .apply(ParDo.of(new AssignRecordKeyFn<String, Integer>(2)))
            .apply(GroupByKey.create())
            .apply(MapElements.into(integers()).via(KV::getKey));

    PAssert.that(output).containsInAnyOrder(ImmutableList.of(0, 1));

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testAssignOutputShardFnDeterministic() {
    List<KafkaRecord<String, Integer>> inputs = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      inputs.addAll(SAME_OFFSET_INPUTS);
    }

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(inputs)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<Integer> output =
        input
            .apply(ParDo.of(new AssignOffsetShardFn<String, Integer>(1024)))
            .apply(GroupByKey.create())
            .apply(MapElements.into(integers()).via(KV::getKey));

    PCollection<Long> count = output.apply("CountElements", Count.globally());
    PAssert.that(count).containsInAnyOrder(1L);

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testAssignRecordKeyFnDeterministic() {
    List<KafkaRecord<String, Integer>> inputs = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      inputs.addAll(SAME_KEY_INPUTS);
    }

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(inputs)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<Integer> output =
        input
            .apply(ParDo.of(new AssignRecordKeyFn<String, Integer>(1024)))
            .apply(GroupByKey.create())
            .apply(MapElements.into(integers()).via(KV::getKey));

    PCollection<Long> count = output.apply("CountElements", Count.globally());
    PAssert.that(count).containsInAnyOrder(1L);

    pipeline.run();
  }
}
