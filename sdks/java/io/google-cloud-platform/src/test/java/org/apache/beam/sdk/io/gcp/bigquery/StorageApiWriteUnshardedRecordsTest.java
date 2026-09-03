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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for propagation of fused downstream failures from Storage Write API success output. */
@RunWith(Parameterized.class)
public class StorageApiWriteUnshardedRecordsTest {
  @Parameters(name = "flushDuringProcessElement={0}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  @Parameter public boolean flushDuringProcessElement;

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
    BigQueryIO.clearStaticCaches();
  }

  @After
  public void tearDown() throws Exception {
    BigQueryIO.clearStaticCaches();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSuccessfulOutputPropagatesDownstreamFailure() throws Exception {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setProject("project-id");
    if (flushDuringProcessElement) {
      options.setStorageApiAppendThresholdRecordCount(0);
    }
    TableReference table =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    TableSchema schema =
        new TableSchema()
            .setFields(ImmutableList.of(new TableFieldSchema().setName("id").setType("STRING")));
    FakeDatasetService dataset = new FakeDatasetService();
    dataset.createDataset("project-id", "dataset-id", "", "", null);
    dataset.createTable(new Table().setTableReference(table).setSchema(schema));

    Pipeline pipeline = Pipeline.create(options);
    WriteResult writeResult =
        pipeline
            .apply(Create.of(row("first"), row("second")).withCoder(TableRowJsonCoder.of()))
            .apply(
                BigQueryIO.writeTableRows()
                    .to(table)
                    .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withNumStorageWriteApiStreams(0)
                    .withPropagateSuccessfulStorageApiWrites(true)
                    .withoutValidation()
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(dataset)
                            .withJobService(new FakeJobService())));
    writeResult
        .getSuccessfulStorageApiInserts()
        .apply("FailAfterBigQuery", ParDo.of(new FailFirstRow()));

    // DirectRunner buffers outputs between ParDos. Use the SDK harness to exercise the actual
    // synchronous output call into a fused consumer, including its bundle failure handling.
    SdkComponents components = SdkComponents.create(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, components);
    Map.Entry<String, RunnerApi.PTransform> writer =
        getOnlyElement(
            pipelineProto.getComponents().getTransformsMap().entrySet().stream()
                .filter(entry -> entry.getValue().getSubtransformsCount() == 0)
                .filter(entry -> entry.getValue().getUniqueName().endsWith("Write Records"))
                .collect(Collectors.toList()));
    Map.Entry<String, RunnerApi.PTransform> downstream =
        getOnlyElement(
            pipelineProto.getComponents().getTransformsMap().entrySet().stream()
                .filter(entry -> entry.getValue().getSubtransformsCount() == 0)
                .filter(entry -> entry.getValue().getUniqueName().startsWith("FailAfterBigQuery"))
                .collect(Collectors.toList()));
    assertTrue(
        writer
            .getValue()
            .getOutputsMap()
            .containsValue(getOnlyElement(downstream.getValue().getInputsMap().values())));
    assertTrue(
        GreedyPipelineFuser.fuse(pipelineProto).getFusedStages().stream()
            .anyMatch(
                stage ->
                    stage.getTransforms().stream()
                            .anyMatch(transform -> transform.getId().equals(writer.getKey()))
                        && stage.getTransforms().stream()
                            .anyMatch(transform -> transform.getId().equals(downstream.getKey()))));

    String inputId = getOnlyElement(writer.getValue().getInputsMap().values());
    Coder<KV<TableDestination, StorageApiWritePayload>> inputCoder =
        (Coder<KV<TableDestination, StorageApiWritePayload>>)
            RehydratedComponents.forComponents(pipelineProto.getComponents())
                .getCoder(
                    pipelineProto.getComponents().getPcollectionsOrThrow(inputId).getCoderId());
    Coder<WindowedValue<KV<TableDestination, StorageApiWritePayload>>> windowedCoder =
        WindowedValues.getFullCoder(inputCoder, GlobalWindow.Coder.INSTANCE);
    String coderId = components.registerCoder(windowedCoder);
    BeamFnApi.ProcessBundleDescriptor bundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .setId("fused-write")
            .putAllCoders(components.toComponents().getCodersMap())
            .putAllWindowingStrategies(pipelineProto.getComponents().getWindowingStrategiesMap())
            .putAllPcollections(pipelineProto.getComponents().getPcollectionsMap())
            .putAllEnvironments(pipelineProto.getComponents().getEnvironmentsMap())
            .putTransforms(writer.getKey(), writer.getValue())
            .putTransforms(downstream.getKey(), downstream.getValue())
            .putTransforms(
                "input",
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("input")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(RemoteGrpcPortRead.URN)
                            .setPayload(
                                BeamFnApi.RemoteGrpcPort.newBuilder()
                                    .setCoderId(coderId)
                                    .setApiServiceDescriptor(
                                        Endpoints.ApiServiceDescriptor.newBuilder()
                                            .setUrl("embedded-input"))
                                    .build()
                                    .toByteString()))
                    .putOutputs("out", inputId)
                    .build())
            .build();

    Descriptor rowDescriptor =
        TableRowToStorageApiProto.wrapDescriptorProto(
            TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
                TableRowToStorageApiProto.schemaToProtoTableSchema(schema), true, false));
    ByteArrayOutputStream input = new ByteArrayOutputStream();
    for (String id : ImmutableList.of("first", "second")) {
      byte[] message =
          DynamicMessage.newBuilder(rowDescriptor)
              .setField(rowDescriptor.findFieldByName("id"), id)
              .build()
              .toByteArray();
      windowedCoder.encode(
          WindowedValues.valueInGlobalWindow(
              KV.of(
                  new TableDestination(table, null),
                  StorageApiWritePayload.of(message, null, null))),
          input);
    }
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("bundle-1")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId(bundleDescriptor.getId())
                    .setElements(
                        BeamFnApi.Elements.newBuilder()
                            .addData(
                                BeamFnApi.Elements.Data.newBuilder()
                                    .setTransformId("input")
                                    .setInstructionId("bundle-1")
                                    .setData(ByteString.copyFrom(input.toByteArray()))
                                    .setIsLast(true))))
            .build();
    BeamFnDataClient dataClient = mock(BeamFnDataClient.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(options, System::currentTimeMillis, null);
    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            options,
            Collections.emptySet(),
            ignored -> bundleDescriptor,
            dataClient,
            null,
            null,
            new ShortIdMap(),
            sampler,
            Caches.noop(),
            null);
    try {
      UserCodeException failure =
          assertThrows(UserCodeException.class, () -> handler.processBundle(request));
      assertEquals("Downstream failed on first row", failure.getCause().getMessage());
      verify(dataClient).poisonInstructionId("bundle-1");
      assertThat(
          dataset.getAllRows("project-id", "dataset-id", "table-id"),
          flushDuringProcessElement
              ? containsInAnyOrder(row("first"))
              : containsInAnyOrder(row("first"), row("second")));
    } finally {
      handler.shutdown();
      sampler.stop();
    }
  }

  private static TableRow row(String id) {
    return new TableRow().set("id", id);
  }

  private static class FailFirstRow extends DoFn<TableRow, TableRow> {
    @ProcessElement
    public void process(@Element TableRow row, OutputReceiver<TableRow> receiver) {
      if (row.get("id").equals("first")) {
        throw new IllegalStateException("Downstream failed on first row");
      }
      receiver.output(row);
    }
  }
}
