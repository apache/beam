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

  private static class ThrowingFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement() {
      throw new IllegalStateException("boom");
    }
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
  public void testManagedIcebergWriteEmitsDatasetWithSymlink() throws Exception {
    String warehouse = "file://" + temporaryFolder.newFolder("e2e-warehouse").getAbsolutePath();
    org.apache.iceberg.catalog.TableIdentifier tableId =
        org.apache.iceberg.catalog.TableIdentifier.parse("demo.orders");
    try (org.apache.iceberg.hadoop.HadoopCatalog catalog =
        new org.apache.iceberg.hadoop.HadoopCatalog(
            new org.apache.hadoop.conf.Configuration(), warehouse)) {
      catalog.createTable(
          tableId,
          new org.apache.iceberg.Schema(
              org.apache.iceberg.types.Types.NestedField.required(
                  1, "id", org.apache.iceberg.types.Types.LongType.get()),
              org.apache.iceberg.types.Types.NestedField.required(
                  2, "name", org.apache.iceberg.types.Types.StringType.get())));
    }
    java.util.Map<String, Object> config = new java.util.HashMap<>();
    config.put("table", "demo.orders");
    config.put("catalog_name", "local");
    java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
    catalogProps.put("type", "hadoop");
    catalogProps.put("warehouse", warehouse);
    config.put("catalog_properties", catalogProps);

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(OpenLineageRunner.class);
    org.apache.beam.sdk.schemas.Schema beamSchema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .build();

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.of(
                    org.apache.beam.sdk.values.Row.withSchema(beamSchema)
                        .addValues(1L, "laptop")
                        .build())
                .withRowSchema(beamSchema))
        .apply(
            org.apache.beam.sdk.managed.Managed.write(org.apache.beam.sdk.managed.Managed.ICEBERG)
                .withConfig(config));
    pipeline.run().waitUntilFinish();

    List<OpenLineage.RunEvent> events = awaitTerminalEvent();
    // The graph-extracted Iceberg dataset must be on the START event already.
    OpenLineage.OutputDataset output = events.get(0).getOutputs().get(0);
    assertEquals("file", output.getNamespace());
    assertTrue(output.getName().endsWith("/e2e-warehouse/demo/orders"));
    OpenLineage.SymlinksDatasetFacetIdentifiers symlink =
        output.getFacets().getSymlinks().getIdentifiers().get(0);
    assertEquals("demo.orders", symlink.getName());
    assertEquals("TABLE", symlink.getType());
  }

  @Test
  public void testStreamingPipelineEmitsRunningHeartbeatsAndAbortOnCancel() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs("--blockOnRun=false").create();
    options.setRunner(OpenLineageRunner.class);
    options.as(OpenLineagePipelineOptions.class).setOpenLineageTrackingIntervalInSeconds(1);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(org.apache.beam.sdk.io.GenerateSequence.from(0));
    PipelineResult result = pipeline.run();

    // The tracker must emit periodic RUNNING events while the job runs...
    awaitEventCount(OpenLineage.RunEvent.EventType.RUNNING, 2);
    // ...and CANCELLED must map to ABORT, per the Flink integration's terminal mapping.
    result.cancel();
    awaitEventCount(OpenLineage.RunEvent.EventType.ABORT, 1);
  }

  @Test
  public void testFailingPipelineEmitsFailWithErrorMessageFacet() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(OpenLineageRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1)).apply(ParDo.of(new ThrowingFn()));
    try {
      pipeline.run().waitUntilFinish();
      throw new AssertionError("pipeline should have failed");
    } catch (RuntimeException expected) {
      // expected
    }

    List<OpenLineage.RunEvent> events = awaitEventCount(OpenLineage.RunEvent.EventType.FAIL, 1);
    OpenLineage.RunEvent fail = events.get(events.size() - 1);
    String message = fail.getRun().getFacets().getErrorMessage().getMessage();
    assertTrue("errorMessage was: " + message, message.contains("boom"));
    assertEquals("JAVA", fail.getRun().getFacets().getErrorMessage().getProgrammingLanguage());
  }

  private List<OpenLineage.RunEvent> awaitEventCount(
      OpenLineage.RunEvent.EventType type, int minCount) throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    List<OpenLineage.RunEvent> events = readEvents();
    while (System.currentTimeMillis() < deadline) {
      events = readEvents();
      if (events.stream().filter(e -> e.getEventType() == type).count() >= minCount) {
        return events;
      }
      Thread.sleep(250);
    }
    throw new AssertionError("Expected " + minCount + " " + type + " events; got " + events);
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
