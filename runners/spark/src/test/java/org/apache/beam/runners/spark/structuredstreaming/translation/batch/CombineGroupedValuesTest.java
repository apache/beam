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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link Combine#groupedValues} translation. */
@RunWith(JUnit4.class)
public class CombineGroupedValuesTest implements Serializable {
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.fromOptions(SESSION.createPipelineOptions());

  @Test
  public void testCombineGroupedValues() {
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(
                Create.<KV<String, Iterable<Integer>>>of(
                        KV.of("a", ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
                        KV.of("b", ImmutableList.of()))
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(VarIntCoder.of()))))
            .apply(Combine.groupedValues(Sum.ofIntegers()));

    PAssert.that(input).containsInAnyOrder(KV.of("a", 55), KV.of("b", 0));
    pipeline.run();
  }
}
