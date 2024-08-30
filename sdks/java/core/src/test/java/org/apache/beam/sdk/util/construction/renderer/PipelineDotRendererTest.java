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
package org.apache.beam.sdk.util.construction.renderer;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineDotRenderer}. */
@RunWith(JUnit4.class)
public class PipelineDotRendererTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testEmptyPipeline() {
    assertEquals(
        "digraph {"
            + "    rankdir=LR"
            + "    subgraph cluster_0 {"
            + "        label = \"\""
            + "    }"
            + "}",
        PipelineDotRenderer.toDotString(p).replaceAll(System.lineSeparator(), ""));
  }

  @Test
  public void testCompositePipeline() {
    p.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                TimestampedValue.of(KV.of(2, 2), new Instant(2))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());
    assertEquals(
        "digraph {"
            + "    rankdir=LR"
            + "    subgraph cluster_0 {"
            + "        label = \"\""
            + "        subgraph cluster_1 {"
            + "            label = \"Create.TimestampedValues\""
            + "            subgraph cluster_2 {"
            + "                label = \"Create.TimestampedValues/Create.Values\""
            + "                subgraph cluster_3 {"
            + "                    label = \"Create.TimestampedValues/Create.Values/Read(CreateSource)\""
            + "                    4 [label=\"Impulse\"]"
            + "                    subgraph cluster_5 {"
            + "                        label = \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(OutputSingleSource)\""
            + "                        6 [label=\"ParMultiDo(OutputSingleSource)\"]"
            + "                        4 -> 6 [style=solid label=\"\"]"
            + "                    }"
            + "                    subgraph cluster_7 {"
            + "                        label = \"Create.TimestampedValues/Create.Values/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)\""
            + "                        8 [label=\"ParMultiDo(BoundedSourceAsSDFWrapper)\"]"
            + "                        6 -> 8 [style=solid label=\"\"]"
            + "                    }"
            + "                }"
            + "            }"
            + "            subgraph cluster_9 {"
            + "                label = \"Create.TimestampedValues/ParDo(ConvertTimestamps)\""
            + "                10 [label=\"ParMultiDo(ConvertTimestamps)\"]"
            + "                8 -> 10 [style=solid label=\"\"]"
            + "            }"
            + "        }"
            + "        subgraph cluster_11 {"
            + "            label = \"Window.Into()\""
            + "            12 [label=\"Window.Assign\"]"
            + "            10 -> 12 [style=solid label=\"\"]"
            + "        }"
            + "        subgraph cluster_13 {"
            + "            label = \"Combine.perKey(SumInteger)\""
            + "            14 [label=\"GroupByKey\"]"
            + "            12 -> 14 [style=solid label=\"\"]"
            + "            subgraph cluster_15 {"
            + "                label = \"Combine.perKey(SumInteger)/Combine.GroupedValues\""
            + "                subgraph cluster_16 {"
            + "                    label = \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\""
            + "                    17 [label=\"ParMultiDo(Anonymous)\"]"
            + "                    14 -> 17 [style=solid label=\"\"]"
            + "                }"
            + "            }"
            + "        }"
            + "    }"
            + "}",
        PipelineDotRenderer.toDotString(p).replaceAll(System.lineSeparator(), ""));
  }
}
