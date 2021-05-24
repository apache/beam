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
package org.apache.beam.runners.samza.renderer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
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

/** Tests for {@link PipelineJsonRenderer}. */
@RunWith(JUnit4.class)
public class PipelineJsonRendererTest {

  static {
    System.setProperty("beamUseDummyRunner", Boolean.TRUE.toString());
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testEmptyPipeline() {
    System.out.println(PipelineJsonRenderer.toJsonString(p));
    assertEquals(
        "{  \"RootNode\": ["
            + "    { \"fullName\":\"OuterMostNode\","
            + "      \"shortName\":\"OuterMostNode\","
            + "      \"id\":\"OuterMostNode\","
            + "      \"ChildNode\":[    ]}],\"graphLinks\": []"
            + "}",
        PipelineJsonRenderer.toJsonString(p).replaceAll(System.lineSeparator(), ""));
  }

  @Test
  public void testCompositePipeline() throws IOException {

    p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());

    String jsonDagFileName = "src/test/resources/ExpectedDag.json";
    String jsonDag =
        new String(Files.readAllBytes(Paths.get(jsonDagFileName)), Charset.defaultCharset());

    assertEquals(
        jsonDag.replaceAll("\\s+", ""),
        PipelineJsonRenderer.toJsonString(p)
            .replaceAll(System.lineSeparator(), "")
            .replaceAll("\\s+", ""));
  }
}
