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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.runners.samza.SamzaExecutionEnvironment;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.translation.ConfigBuilder;
import org.apache.beam.runners.samza.translation.ConfigContext;
import org.apache.beam.runners.samza.translation.PViewToIdMapper;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.beam.runners.samza.translation.SamzaTransformOverrides;
import org.apache.beam.runners.samza.translation.StateIdParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PValue;
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
    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(p);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(p);
    final ConfigContext ctx = new ConfigContext(idMap, nonUniqueStateIds, options);

    String jsonDag =
        "{  \"RootNode\": ["
            + "    { \"fullName\":\"OuterMostNode\","
            + "      \"ChildNodes\":[    ]}],\"graphLinks\": [],\"transformIOInfo\": []"
            + "}";

    assertEquals(
        JsonParser.parseString(jsonDag),
        JsonParser.parseString(
            PipelineJsonRenderer.toJsonString(p, ctx).replaceAll(System.lineSeparator(), "")));
  }

  @Test
  public void testCompositePipeline() throws IOException {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setRunner(SamzaRunner.class);
    options.setJobName("TestEnvConfig");
    options.setSamzaExecutionEnvironment(SamzaExecutionEnvironment.LOCAL);

    Pipeline p = Pipeline.create(options);

    p.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                TimestampedValue.of(KV.of(2, 2), new Instant(2))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());

    p.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(p);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(p);
    final ConfigContext ctx = new ConfigContext(idMap, nonUniqueStateIds, options);

    String jsonDagFileName = "src/test/resources/ExpectedDag.json";
    String jsonDag =
        new String(Files.readAllBytes(Paths.get(jsonDagFileName)), StandardCharsets.UTF_8);
    String renderedDag = PipelineJsonRenderer.toJsonString(p, ctx);

    assertEquals(
        JsonParser.parseString(jsonDag),
        JsonParser.parseString(renderedDag.replaceAll(System.lineSeparator(), "")));
  }

  @Test
  public void testBeamTransformIOConfigGen() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestEnvConfig");
    options.setRunner(SamzaRunner.class);
    options.setSamzaExecutionEnvironment(SamzaExecutionEnvironment.LOCAL);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Impulse.create()).apply(Filter.by(Objects::nonNull));
    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);

    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Map<String, Map.Entry<String, String>> transformInputOutput =
        PipelineJsonRenderer.buildTransformIOMap(pipeline, configCtx);

    assertEquals(2, transformInputOutput.size());
    assertEquals("", transformInputOutput.get("Impulse").getKey()); // no input to impulse
    assertEquals(
        "Impulse.out",
        transformInputOutput.get("Impulse").getValue()); // PValue for to Impulse.output

    // Input to Filter is PValue Output from Impulse
    assertEquals(
        "Impulse.out",
        transformInputOutput.get("Filter/ParDo(Anonymous)/ParMultiDo(Anonymous)").getKey());
    // output PValue of filter
    assertEquals(
        "Filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output",
        transformInputOutput.get("Filter/ParDo(Anonymous)/ParMultiDo(Anonymous)").getValue());
  }
}
