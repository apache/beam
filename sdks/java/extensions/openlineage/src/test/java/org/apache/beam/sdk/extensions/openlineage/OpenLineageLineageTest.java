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
package org.apache.beam.sdk.extensions.openlineage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.FileConfig;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.lineage.LineageOptions;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end test for the {@link OpenLineageLineage} plugin: activated via {@code lineageType}, it
 * must capture lineage live from worker code and tee it back into the metrics store.
 */
@RunWith(JUnit4.class)
public class OpenLineageLineageTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File eventsFile;

  @Before
  public void setUp() {
    eventsFile = new File(temporaryFolder.getRoot(), "plugin-events.jsonl");
    BeamOpenLineageConfig config = new BeamOpenLineageConfig();
    config.setTransportConfig(new FileConfig(eventsFile.getAbsolutePath()));
    OpenLineageContext.resetForTests();
    OpenLineageContext.overrideConfigForTests(config);
  }

  @After
  public void tearDown() {
    OpenLineageContext.resetForTests();
  }

  /** Mirrors the runtime Lineage calls IO connectors make. */
  private static class ReportingFn extends DoFn<Integer, Integer> {
    private transient boolean reported;

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (!reported) {
        reported = true;
        Lineage.getSources().add("kafka", Arrays.asList("broker-1:9092,broker-2:9092", "payments"));
      }
      context.output(context.element());
    }
  }

  @Test
  public void testPluginCapturesLineageLiveAndTeesToMetrics() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(LineageOptions.class).setLineageType(OpenLineageLineage.class);
    options.as(OpenLineagePipelineOptions.class).setOpenLineageJobName("plugin_job");

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(new ReportingFn()));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // The plugin emitted events live (START plus at least one RUNNING with the dataset).
    List<OpenLineage.RunEvent> events = awaitDatasetEvent();
    assertEquals(OpenLineage.RunEvent.EventType.START, events.get(0).getEventType());
    OpenLineage.RunEvent last = events.get(events.size() - 1);
    assertEquals("kafka://broker-1:9092", last.getInputs().get(0).getNamespace());
    assertEquals("payments", last.getInputs().get(0).getName());
    assertEquals("plugin_job", last.getJob().getName());

    // The tee keeps the metrics-based lineage store populated for other consumers.
    Set<String> sources = Lineage.query(result.metrics(), Lineage.Type.SOURCE);
    assertFalse(sources.isEmpty());
    assertTrue(
        sources.toString(), sources.contains("kafka:`broker-1:9092,broker-2:9092`.payments"));
  }

  private List<OpenLineage.RunEvent> awaitDatasetEvent() throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    List<OpenLineage.RunEvent> events = null;
    while (System.currentTimeMillis() < deadline) {
      if (eventsFile.exists()) {
        events =
            Files.readAllLines(eventsFile.toPath(), StandardCharsets.UTF_8).stream()
                .filter(line -> !line.isEmpty())
                .map(OpenLineageClientUtils::runEventFromJson)
                .collect(Collectors.toList());
        if (!events.isEmpty() && !events.get(events.size() - 1).getInputs().isEmpty()) {
          return events;
        }
      }
      Thread.sleep(250);
    }
    throw new AssertionError("No event with datasets observed; got " + events);
  }
}
