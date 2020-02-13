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
package org.apache.beam.runners.core.construction.renderer;

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
    p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
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
            + "                3 [label=\"Read(CreateSource)\"]"
            + "            }"
            + "            subgraph cluster_4 {"
            + "                label = \"Create.TimestampedValues/ParDo(ConvertTimestamps)\""
            + "                5 [label=\"ParMultiDo(ConvertTimestamps)\"]"
            + "                3 -> 5 [style=solid label=\"\"]"
            + "            }"
            + "        }"
            + "        subgraph cluster_6 {"
            + "            label = \"Window.Into()\""
            + "            7 [label=\"Window.Assign\"]"
            + "            5 -> 7 [style=solid label=\"\"]"
            + "        }"
            + "        subgraph cluster_8 {"
            + "            label = \"Combine.perKey(SumInteger)\""
            + "            9 [label=\"GroupByKey\"]"
            + "            7 -> 9 [style=solid label=\"\"]"
            + "            subgraph cluster_10 {"
            + "                label = \"Combine.perKey(SumInteger)/Combine.GroupedValues\""
            + "                subgraph cluster_11 {"
            + "                    label = \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\""
            + "                    12 [label=\"ParMultiDo(Anonymous)\"]"
            + "                    9 -> 12 [style=solid label=\"\"]"
            + "                }"
            + "            }"
            + "        }"
            + "    }"
            + "}",
        PipelineDotRenderer.toDotString(p).replaceAll(System.lineSeparator(), ""));
  }
}
