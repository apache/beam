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
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PortablePipelineDotRenderer}. */
@RunWith(JUnit4.class)
public class PortablePipelineDotRendererTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testEmptyPipeline() {
    assertEquals(
        "digraph {" + "    rankdir=LR" + "}",
        PortablePipelineDotRenderer.toDotString(PipelineTranslation.toProto(p))
            .replaceAll(System.lineSeparator(), ""));
  }

  @Test
  public void testCompositePipeline() {
    p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());

    assertEquals(
        "digraph {"
            + "    rankdir=LR"
            + "    0 [label=\"Create.TimestampedValues\\n\"]"
            + "    1 [label=\"Window.Into()\\n\"]"
            + "    0 -> 1 [style=solid label=\"Create.TimestampedValues/ParDo(ConvertTimestamps)/ParMultiDo(ConvertTimestamps).output\"]"
            + "    2 [label=\"Combine.perKey(SumInteger)\\nbeam:transform:combine_per_key:v1\"]"
            + "    1 -> 2 [style=solid label=\"Window.Into()/Window.Assign.out\"]"
            + "}",
        PortablePipelineDotRenderer.toDotString(PipelineTranslation.toProto(p))
            .replaceAll(System.lineSeparator(), ""));
  }
}
