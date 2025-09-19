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
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
          MakeKafkaRecord("k1", 3, 1),
          MakeKafkaRecord("k5", Integer.MAX_VALUE, 2),
          MakeKafkaRecord("k5", Integer.MIN_VALUE, 3),
          MakeKafkaRecord("k2", 66, 4),
          MakeKafkaRecord("k1", 4, 5),
          MakeKafkaRecord("k2", -33, 6),
          MakeKafkaRecord("k3", 0, 7));

  static KafkaRecord<String, Integer> MakeKafkaRecord(String key, Integer value, Integer offset) {
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
  public void testJustRedistribute() {

    PCollection<KafkaRecord<String, Integer>> input =
        pipeline.apply(
            Create.of(INPUTS)
                .withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KafkaRecord<String, Integer>> output =
        input.apply(KafkaReadRedistribute.redistribute());

    PAssert.that(output).containsInAnyOrder(INPUTS);

    assertEquals(input.getWindowingStrategy(), output.getWindowingStrategy());

    pipeline.run();
  }
}
