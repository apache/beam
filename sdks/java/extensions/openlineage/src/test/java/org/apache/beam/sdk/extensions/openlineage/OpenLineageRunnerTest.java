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
import static org.junit.Assert.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.FileConfig;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
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
 * End-to-end test for {@link OpenLineageRunner}: runs a pipeline on the direct runner through the
 * wrapper with a file transport and asserts the emitted event sequence, mirroring the event-file
 * assertions of the Spark and Flink integration tests.
 */
@RunWith(JUnit4.class)
public class OpenLineageRunnerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File eventsFile;

  @Before
  public void setUp() throws Exception {
    eventsFile = new File(temporaryFolder.getRoot(), "events.jsonl");
    BeamOpenLineageConfig config = new BeamOpenLineageConfig();
    config.setTransportConfig(new FileConfig(eventsFile.getAbsolutePath()));
    OpenLineageContext.resetForTests();
    OpenLineageContext.overrideConfigForTests(config);
  }

  @After
  public void tearDown() {
    OpenLineageContext.resetForTests();
  }

  /** Mirrors the runtime Lineage calls IO connectors make (e.g. PubsubIO.java). */
  private static class ReportingFn extends DoFn<Integer, Integer> {
    private transient boolean reported;

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (!reported) {
        reported = true;
        Lineage.getSources()
            .add("pubsub", "topic", Arrays.asList("acme-prod", "orders-events"), null);
        Lineage.getSinks().add("bigquery", Arrays.asList("acme-prod", "sales", "orders"));
      }
      context.output(context.element());
    }
  }

  @Test
  public void testStartAndCompleteEventsWithSweptDatasets() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(OpenLineageRunner.class);
    options.as(OpenLineagePipelineOptions.class).setOpenLineageTrackingIntervalInSeconds(1);
    options.as(OpenLineagePipelineOptions.class).setOpenLineageNamespace("test_namespace");
    options.as(OpenLineagePipelineOptions.class).setOpenLineageJobName("test_job");

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(new ReportingFn()));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    List<OpenLineage.RunEvent> events = awaitTerminalEvent();
    assertEquals(OpenLineage.RunEvent.EventType.START, events.get(0).getEventType());
    OpenLineage.RunEvent terminal = events.get(events.size() - 1);
    assertEquals(OpenLineage.RunEvent.EventType.COMPLETE, terminal.getEventType());

    // All events belong to one run, identified by the driver-minted UUID.
    assertEquals(1, events.stream().map(e -> e.getRun().getRunId()).distinct().count());
    // Job identity comes from the pipeline options.
    assertEquals("test_namespace", terminal.getJob().getNamespace());
    assertEquals("test_job", terminal.getJob().getName());
    // The jobType facet marks this integration.
    assertEquals("BEAM", terminal.getJob().getFacets().getJobType().getIntegration());
    // Runtime lineage swept from metrics lands on the terminal event.
    assertEquals("topic:acme-prod:orders-events", terminal.getInputs().get(0).getName());
    assertEquals("pubsub", terminal.getInputs().get(0).getNamespace());
    assertEquals("acme-prod.sales.orders", terminal.getOutputs().get(0).getName());
    assertEquals("bigquery", terminal.getOutputs().get(0).getNamespace());
  }

  @Test
  public void testParentRunFacetAttachedWhenFullyConfigured() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(OpenLineageRunner.class);
    OpenLineagePipelineOptions olOptions = options.as(OpenLineagePipelineOptions.class);
    olOptions.setOpenLineageParentRunId("11111111-2222-3333-4444-555555555555");
    olOptions.setOpenLineageParentJobName("parent_dag.parent_task");
    olOptions.setOpenLineageParentJobNamespace("airflow_namespace");

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1));
    pipeline.run().waitUntilFinish();

    List<OpenLineage.RunEvent> events = awaitTerminalEvent();
    OpenLineage.ParentRunFacet parent = events.get(0).getRun().getFacets().getParent();
    assertEquals("11111111-2222-3333-4444-555555555555", parent.getRun().getRunId().toString());
    assertEquals("parent_dag.parent_task", parent.getJob().getName());
    assertEquals("airflow_namespace", parent.getJob().getNamespace());
  }

  @Test
  public void testDisabledOptionSuppressesAllEvents() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(OpenLineageRunner.class);
    options.as(OpenLineagePipelineOptions.class).setOpenLineageDisabled(true);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1));
    pipeline.run().waitUntilFinish();
    Thread.sleep(2000);

    assertTrue(!eventsFile.exists() || readEvents().isEmpty());
  }

  private List<OpenLineage.RunEvent> awaitTerminalEvent() throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      List<OpenLineage.RunEvent> events = readEvents();
      if (!events.isEmpty()
          && events.get(events.size() - 1).getEventType()
              == OpenLineage.RunEvent.EventType.COMPLETE) {
        return events;
      }
      Thread.sleep(250);
    }
    throw new AssertionError("No COMPLETE event observed in " + readEvents());
  }

  private List<OpenLineage.RunEvent> readEvents() throws Exception {
    if (!eventsFile.exists()) {
      return new ArrayList<>();
    }
    return Files.readAllLines(eventsFile.toPath(), StandardCharsets.UTF_8).stream()
        .filter(line -> !line.isEmpty())
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }
}
