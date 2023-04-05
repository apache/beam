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
package org.apache.beam.runners.samza.util;

import static org.junit.Assert.assertEquals;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link org.apache.beam.runners.samza.util.PipelineJsonRenderer}. */
public class PipelineJsonRendererTest {

  @Test
  public void testEmptyPipeline() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setRunner(SamzaRunner.class);

    Pipeline p = Pipeline.create(options);

    String jsonDag =
        "{  \"RootNode\": ["
            + "    { \"fullName\":\"OuterMostNode\","
            + "      \"ChildNodes\":[    ]}],\"graphLinks\": []"
            + "}";

    System.out.println(PipelineJsonRenderer.toJsonString(p));
    assertEquals(
        JsonParser.parseString(jsonDag),
        JsonParser.parseString(
            PipelineJsonRenderer.toJsonString(p).replaceAll(System.lineSeparator(), "")));
  }

  @Test
  public void testCompositePipeline() throws IOException {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setRunner(SamzaRunner.class);

    Pipeline p = Pipeline.create(options);

    p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());

    String jsonDagFileName = "src/test/resources/ExpectedDag.json";
    String jsonDag =
        new String(Files.readAllBytes(Paths.get(jsonDagFileName)), StandardCharsets.UTF_8);

    assertEquals(
        JsonParser.parseString(jsonDag),
        JsonParser.parseString(
            PipelineJsonRenderer.toJsonString(p).replaceAll(System.lineSeparator(), "")));
  }
}
