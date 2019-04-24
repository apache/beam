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

import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link ParDo} translation. */
@RunWith(JUnit4.class)
public class GroupByKeyTest implements Serializable {
  private static Pipeline p;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptions options = PipelineOptionsFactory.create().as(PipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    p = Pipeline.create(options);
  }

  @Test
  public void testGroupByKey() {
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    PCollection<KV<Integer, Iterable<Integer>>> input =
        p.apply(Create.of(elems)).apply(GroupByKey.create());
    PAssert.thatMap(input)
        .satisfies(
            results -> {
              assertThat(results.get(1), containsInAnyOrder(1, 3, 5));
              assertThat(results.get(2), containsInAnyOrder(2, 4, 6));
              return null;
            });
    p.run();
  }
}
