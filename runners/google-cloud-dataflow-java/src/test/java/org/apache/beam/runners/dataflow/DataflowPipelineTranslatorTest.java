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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.util.StringUtils.jsonStringToByteArray;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.Step;
import com.google.api.services.dataflow.model.WorkerPool;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ArtifactInformation;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.DockerPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHintsOptions;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;

/** Tests for DataflowPipelineTranslator. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class DataflowPipelineTranslatorTest implements Serializable {

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private SdkComponents createSdkComponents(PipelineOptions options) {
    SdkComponents sdkComponents = SdkComponents.create();

    String containerImageURL =
        DataflowRunner.getContainerImageForJob(options.as(DataflowPipelineOptions.class));
    RunnerApi.Environment defaultEnvironmentForDataflow =
        Environments.createDockerEnvironment(containerImageURL);

    sdkComponents.registerEnvironment(defaultEnvironmentForDataflow);
    return sdkComponents;
  }

  // A Custom Mockito matcher for an initial Job that checks that all
  // expected fields are set.
  private static class IsValidCreateRequest implements ArgumentMatcher<Job> {

    @Override
    public boolean matches(Job o) {
      Job job = (Job) o;
      return job.getId() == null
          && job.getProjectId() == null
          && job.getName() != null
          && job.getType() != null
          && job.getEnvironment() != null
          && job.getSteps() != null
          && job.getCurrentState() == null
          && job.getCurrentStateTime() == null
          && job.getExecutionInfo() == null
          && job.getCreateTime() == null;
    }
  }

  private Pipeline buildPipeline(DataflowPipelineOptions options) {
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadMyFile", TextIO.read().from("gs://bucket/object"))
        .apply("WriteMyFile", TextIO.write().to("gs://bucket/object"));
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(p);

    return p;
  }

  private static Dataflow buildMockDataflow(ArgumentMatcher<Job> jobMatcher) throws IOException {
    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Jobs mockJobs = mock(Dataflow.Projects.Jobs.class);
    Dataflow.Projects.Jobs.Create mockRequest = mock(Dataflow.Projects.Jobs.Create.class);

    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.jobs()).thenReturn(mockJobs);
    when(mockJobs.create(eq("someProject"), argThat(jobMatcher))).thenReturn(mockRequest);

    Job resultJob = new Job();
    resultJob.setId("newid");
    when(mockRequest.execute()).thenReturn(resultJob);
    return mockDataflowClient;
  }

  private static DataflowPipelineOptions buildPipelineOptions() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.expand(any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));
    doNothing().when(mockGcsUtil).verifyBucketAccessible(any(GcsPath.class));

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setJobName("some-job-name");
    options.setProject("some-project");
    options.setRegion("some-region");
    options.setTempLocation(GcsPath.fromComponents("somebucket", "some/path").toString());
    options.setFilesToStage(new ArrayList<>());
    options.setDataflowClient(buildMockDataflow(new IsValidCreateRequest()));
    options.setGcsUtil(mockGcsUtil);

    // Enable the FileSystems API to know about gs:// URIs in this test.
    FileSystems.setDefaultPipelineOptions(options);

    return options;
  }

  // Test that the transform names for Storage Write API for streaming pipelines are what we expect
  // them to be. This is required since the Windmill backend expects the step to contain that name.
  // For a more stable solution, we should use URN, but that is not currently used in the legacy
  // java
  // worker.
  // TODO:(https://github.com/apache/beam/issues/29338) Pass in URN information to Dataflow Runner.
  @Test
  public void testStorageWriteApiTransformNames() throws IOException, Exception {

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);

    BigQueryIO.Write<String> writeTransform =
        BigQueryIO.<String>write()
            .withFormatFunction(
                (SerializableFunction<String, TableRow>)
                    input1 -> new TableRow().set("description", input1))
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withTriggeringFrequency(org.joda.time.Duration.standardSeconds(5))
            .to(
                new TableReference()
                    .setProjectId("project")
                    .setDatasetId("dataset")
                    .setTableId("table"))
            .withSchema(
                new TableSchema()
                    .setFields(
                        new ArrayList<>(
                            ImmutableList.of(
                                new TableFieldSchema().setName("description").setType("STRING")))));

    p.apply(Create.of("1", "2", "3", "4").withCoder(StringUtf8Coder.of()))
        .setIsBoundedInternal(IsBounded.UNBOUNDED)
        .apply("StorageWriteApi", writeTransform);

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    DataflowPipelineTranslator t =
        DataflowPipelineTranslator.fromOptions(
            PipelineOptionsFactory.as(DataflowPipelineOptions.class));

    JobSpecification jobSpecification =
        t.translate(
            p,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList());

    boolean foundStep = false;
    for (Step step : jobSpecification.getJob().getSteps()) {
      if (getString(step.getProperties(), PropertyNames.USER_NAME)
          .contains("StorageWriteApi/StorageApiLoads")) {
        foundStep = true;
      }
    }
    assertTrue(foundStep);
  }

  // Test that the transform names added for TextIO writes with autosharding. This is required since
  // the Windmill backend expects the file write autosharded step to contain that name.
  // For a more stable solution, we should use URN, but that is not currently used in the legacy
  // java
  // worker.
  // TODO:(https://github.com/apache/beam/issues/29338) Pass in URN information to Dataflow Runner.
  @Test
  public void testGCSWriteTransformNames() throws IOException, Exception {

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);

    p.apply(Create.of("1", "2", "3", "4").withCoder(StringUtf8Coder.of()))
        .setIsBoundedInternal(IsBounded.UNBOUNDED)
        .apply(Window.into(FixedWindows.of(Duration.millis(1))))
        .apply(
            "WriteMyFile",
            TextIO.write().to("gs://bucket/object").withWindowedWrites().withNumShards(0));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    DataflowPipelineTranslator t =
        DataflowPipelineTranslator.fromOptions(
            PipelineOptionsFactory.as(DataflowPipelineOptions.class));

    JobSpecification jobSpecification =
        t.translate(
            p,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList());

    // Assert that at least one of the steps added was the autosharded write. This is added to
    // ensure the name doesn't change.
    boolean foundStep = false;
    for (Step step : jobSpecification.getJob().getSteps()) {
      if (getString(step.getProperties(), PropertyNames.USER_NAME)
          .contains("WriteFiles/WriteAutoShardedBundlesToTempFiles")) {
        foundStep = true;
      }
    }
    assertTrue(foundStep);
  }

  @Test
  public void testNetworkConfig() throws IOException {
    final String testNetwork = "test-network";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setNetwork(testNetwork);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(testNetwork, job.getEnvironment().getWorkerPools().get(0).getNetwork());
  }

  @Test
  public void testNetworkConfigMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(job.getEnvironment().getWorkerPools().get(0).getNetwork());
  }

  @Test
  public void testSubnetworkConfig() throws IOException {
    final String testSubnetwork = "regions/REGION/subnetworks/SUBNETWORK";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setSubnetwork(testSubnetwork);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(testSubnetwork, job.getEnvironment().getWorkerPools().get(0).getSubnetwork());
  }

  @Test
  public void testSubnetworkConfigMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(job.getEnvironment().getWorkerPools().get(0).getSubnetwork());
  }

  @Test
  public void testScalingAlgorithmMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    // Autoscaling settings are always set.
    assertNull(
        job.getEnvironment().getWorkerPools().get(0).getAutoscalingSettings().getAlgorithm());
    assertEquals(
        0,
        job.getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getMaxNumWorkers()
            .intValue());
  }

  @Test
  public void testScalingAlgorithmNone() throws IOException {
    final DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType noScaling =
        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setAutoscalingAlgorithm(noScaling);
    options.setNumWorkers(42);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(
        "AUTOSCALING_ALGORITHM_NONE",
        job.getEnvironment().getWorkerPools().get(0).getAutoscalingSettings().getAlgorithm());
    assertEquals(42, job.getEnvironment().getWorkerPools().get(0).getNumWorkers().intValue());
    assertEquals(
        0,
        job.getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getMaxNumWorkers()
            .intValue());
  }

  @Test
  public void testMaxNumWorkersIsPassedWhenNoAlgorithmIsSet() throws IOException {
    final DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType noScaling = null;
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setMaxNumWorkers(42);
    options.setAutoscalingAlgorithm(noScaling);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(
        job.getEnvironment().getWorkerPools().get(0).getAutoscalingSettings().getAlgorithm());
    assertEquals(
        42,
        job.getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getMaxNumWorkers()
            .intValue());
  }

  @Test
  public void testNumWorkersCannotExceedMaxNumWorkers() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setNumWorkers(43);
    options.setMaxNumWorkers(42);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numWorkers (43) cannot exceed maxNumWorkers (42).");
    DataflowPipelineTranslator.fromOptions(options)
        .translate(
            p,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList())
        .getJob();
  }

  @Test
  public void testWorkerMachineTypeConfig() throws IOException {
    final String testMachineType = "test-machine-type";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setWorkerMachineType(testMachineType);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());

    WorkerPool workerPool = job.getEnvironment().getWorkerPools().get(0);
    assertEquals(testMachineType, workerPool.getMachineType());
  }

  @Test
  public void testDiskSizeGbConfig() throws IOException {
    final Integer diskSizeGb = 1234;

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setDiskSizeGb(diskSizeGb);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(diskSizeGb, job.getEnvironment().getWorkerPools().get(0).getDiskSizeGb());
  }

  /** A composite transform that returns an output that is unrelated to the input. */
  private static class UnrelatedOutputCreator
      extends PTransform<PCollection<Integer>, PCollection<Integer>> {

    @Override
    public PCollection<Integer> expand(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.perElement());

      // Return a value unrelated to the input.
      return input.getPipeline().apply(Create.of(1, 2, 3, 4));
    }
  }

  /** A composite transform that returns an output that is unbound. */
  private static class UnboundOutputCreator extends PTransform<PCollection<Integer>, PDone> {

    @Override
    public PDone expand(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.perElement());

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * A composite transform that returns a partially bound output.
   *
   * <p>This is not allowed and will result in a failure.
   */
  private static class PartiallyBoundOutputCreator
      extends PTransform<PCollection<Integer>, PCollectionTuple> {

    public final TupleTag<Integer> sumTag = new TupleTag<>("sum");
    public final TupleTag<Void> doneTag = new TupleTag<>("done");

    @Override
    public PCollectionTuple expand(PCollection<Integer> input) {
      PCollection<Integer> sum = input.apply(Sum.integersGlobally());

      // Fails here when attempting to construct a tuple with an unbound object.
      return PCollectionTuple.of(sumTag, sum)
          .and(
              doneTag,
              PCollection.createPrimitiveOutputInternal(
                  input.getPipeline(),
                  WindowingStrategy.globalDefault(),
                  input.isBounded(),
                  VoidCoder.of()));
    }
  }

  @Test
  public void testMultiGraphPipelineSerialization() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);

    PCollection<Integer> input = p.begin().apply(Create.of(1, 2, 3));

    input.apply(new UnrelatedOutputCreator());
    input.apply(new UnboundOutputCreator());

    DataflowPipelineTranslator t =
        DataflowPipelineTranslator.fromOptions(
            PipelineOptionsFactory.as(DataflowPipelineOptions.class));

    // Check that translation doesn't fail.
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    JobSpecification jobSpecification =
        t.translate(
            p,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList());
    assertAllStepOutputsHaveUniqueIds(jobSpecification.getJob());
  }

  @Test
  public void testPartiallyBoundFailure() throws IOException {
    Pipeline p = Pipeline.create(buildPipelineOptions());

    PCollection<Integer> input = p.begin().apply(Create.of(1, 2, 3));

    thrown.expect(IllegalArgumentException.class);
    input.apply(new PartiallyBoundOutputCreator());

    Assert.fail("Failure expected from use of partially bound output");
  }

  /** This tests a few corner cases that should not crash. */
  @Test
  public void testGoodWildcards() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(options);

    applyRead(pipeline, "gs://bucket/foo");
    applyRead(pipeline, "gs://bucket/foo/");
    applyRead(pipeline, "gs://bucket/foo/*");
    applyRead(pipeline, "gs://bucket/foo/?");
    applyRead(pipeline, "gs://bucket/foo/[0-9]");
    applyRead(pipeline, "gs://bucket/foo/*baz*");
    applyRead(pipeline, "gs://bucket/foo/*baz?");
    applyRead(pipeline, "gs://bucket/foo/[0-9]baz?");
    applyRead(pipeline, "gs://bucket/foo/baz/*");
    applyRead(pipeline, "gs://bucket/foo/baz/*wonka*");
    applyRead(pipeline, "gs://bucket/foo/*baz/wonka*");
    applyRead(pipeline, "gs://bucket/foo*/baz");
    applyRead(pipeline, "gs://bucket/foo?/baz");
    applyRead(pipeline, "gs://bucket/foo[0-9]/baz");

    // Check that translation doesn't fail.
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    JobSpecification jobSpecification =
        t.translate(
            pipeline,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList());
    assertAllStepOutputsHaveUniqueIds(jobSpecification.getJob());
  }

  private void applyRead(Pipeline pipeline, String path) {
    pipeline.apply("Read(" + path + ")", TextIO.read().from(path));
  }

  private static class TestValueProvider implements ValueProvider<String>, Serializable {

    @Override
    public boolean isAccessible() {
      return false;
    }

    @Override
    public String get() {
      throw new RuntimeException("Should not be called.");
    }
  }

  @Test
  public void testInaccessibleProvider() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(options);

    pipeline.apply(TextIO.read().from(new TestValueProvider()));

    // Check that translation does not fail.
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    t.translate(
        pipeline,
        pipelineProto,
        sdkComponents,
        DataflowRunner.fromOptions(options),
        Collections.emptyList());
  }

  /**
   * Test that in translation the name for a collection (in this case just a Create output) is
   * overridden to be what the Dataflow service expects.
   */
  @Test
  public void testNamesOverridden() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    options.setStreaming(false);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("Jazzy", Create.of(3)).setName("foobizzle");

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();

    // The Create step
    Step step = job.getSteps().get(0);

    // This is the name that is "set by the user" that the Dataflow translator must override
    String userSpecifiedName =
        getString(
            Structs.getListOfMaps(step.getProperties(), PropertyNames.OUTPUT_INFO, null).get(0),
            PropertyNames.USER_NAME);

    // This is the calculated name that must actually be used
    String calculatedName = getString(step.getProperties(), PropertyNames.USER_NAME) + ".out0";

    assertThat(userSpecifiedName, equalTo(calculatedName));
  }

  /**
   * Test that in translation the name for collections of a multi-output ParDo - a special case
   * because the user can name tags - are overridden to be what the Dataflow service expects.
   */
  @Test
  public void testTaggedNamesOverridden() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    options.setStreaming(false);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);

    TupleTag<Integer> tag1 = new TupleTag<Integer>("frazzle") {};
    TupleTag<Integer> tag2 = new TupleTag<Integer>("bazzle") {};
    TupleTag<Integer> tag3 = new TupleTag<Integer>() {};

    PCollectionTuple outputs =
        pipeline
            .apply(Create.of(3, 4))
            .apply(
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void drop() {}
                        })
                    .withOutputTags(tag1, TupleTagList.of(tag2).and(tag3)));

    outputs.get(tag1).setName("bizbazzle");
    outputs.get(tag2).setName("gonzaggle");
    outputs.get(tag3).setName("froonazzle");

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();

    // The ParDo step
    Step step = job.getSteps().get(1);
    String stepName = getString(step.getProperties(), PropertyNames.USER_NAME);

    List<Map<String, Object>> outputInfos =
        Structs.getListOfMaps(step.getProperties(), PropertyNames.OUTPUT_INFO, null);

    assertThat(outputInfos.size(), equalTo(3));

    // The names set by the user _and_ the tags _must_ be ignored, or metrics will not show up.
    for (int i = 0; i < outputInfos.size(); ++i) {
      assertThat(
          getString(outputInfos.get(i), PropertyNames.USER_NAME),
          equalTo(String.format("%s.out%s", stepName, i)));
    }
  }

  /** Smoke test to fail fast if translation of a stateful ParDo in batch breaks. */
  @Test
  public void testBatchStatefulParDoTranslation() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    options.setStreaming(false);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);

    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};

    pipeline
        .apply(Create.of(KV.of(1, 1), KV.of(2, 3)))
        .apply(
            ParDo.of(
                    new DoFn<KV<Integer, Integer>, Integer>() {
                      @StateId("unused")
                      final StateSpec<ValueState<Integer>> stateSpec =
                          StateSpecs.value(VarIntCoder.of());

                      @ProcessElement
                      public void process(ProcessContext c) {
                        // noop
                      }
                    })
                .withOutputTags(mainOutputTag, TupleTagList.empty()));

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();

    // The job should look like:
    // 0. ParallelRead (Create)
    // 1. ParDo(ReifyWVs)
    // 2. GroupByKeyAndSortValuesONly
    // 3. A ParDo over grouped and sorted KVs that is executed via ungrouping service-side

    List<Step> steps = job.getSteps();
    assertEquals(4, steps.size());

    Step createStep = steps.get(0);
    assertEquals("ParallelRead", createStep.getKind());

    Step reifyWindowedValueStep = steps.get(1);
    assertEquals("ParallelDo", reifyWindowedValueStep.getKind());

    Step gbkStep = steps.get(2);
    assertEquals("GroupByKey", gbkStep.getKind());

    Step statefulParDoStep = steps.get(3);
    assertEquals("ParallelDo", statefulParDoStep.getKind());
    assertThat(
        (String) statefulParDoStep.getProperties().get(PropertyNames.USES_KEYED_STATE),
        not(equalTo("true")));
  }

  /** Testing just the translation of the pipeline from ViewTest#testToList. */
  @Test
  public void testToList() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);

    final PCollectionView<List<Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(11, 13, 17, 23))
            .apply(View.<Integer>asList().withRandomAccess());

    pipeline
        .apply("CreateMainInput", Create.of(29, 31))
        .apply(
            "OutputSideInputs",
            ParDo.of(
                    new DoFn<Integer, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        checkArgument(c.sideInput(view).size() == 4);
                        checkArgument(c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                        for (Integer i : c.sideInput(view)) {
                          c.output(i);
                        }
                      }
                    })
                .withSideInputs(view));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();
    List<Step> steps = job.getSteps();

    // Change detector assertion just to make sure the test was not a noop.
    // No need to actually check the pipeline as the ValidatesRunner tests
    // ensure translation is correct. This is just a quick check to see that translation
    // does not crash.
    assertEquals(5, steps.size());
  }

  @Test
  public void testToMap() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();
    List<Step> steps = job.getSteps();

    // Change detector assertion just to make sure the test was not a noop.
    // No need to actually check the pipeline as the ValidatesRunner tests
    // ensure translation is correct. This is just a quick check to see that translation
    // does not crash.
    assertEquals(25, steps.size());
  }

  /** Smoke test to fail fast if translation of a splittable ParDo in streaming breaks. */
  @Test
  public void testStreamingSplittableParDoTranslation() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    options.setStreaming(true);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> windowedInput =
        pipeline
            .apply(Create.of("a"))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));
    windowedInput.apply(ParDo.of(new TestSplittableFn()));

    runner.replaceV1Transforms(pipeline);

    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();

    // The job should contain a SplittableParDo.ProcessKeyedElements step, translated as
    // "SplittableProcessKeyed".

    List<Step> steps = job.getSteps();
    Step processKeyedStep = null;
    for (Step step : steps) {
      if ("SplittableProcessKeyed".equals(step.getKind())) {
        assertNull(processKeyedStep);
        processKeyedStep = step;
      }
    }
    assertNotNull(processKeyedStep);

    @SuppressWarnings({"unchecked", "rawtypes"})
    DoFnInfo<String, Integer> fnInfo =
        (DoFnInfo<String, Integer>)
            SerializableUtils.deserializeFromByteArray(
                jsonStringToByteArray(
                    getString(processKeyedStep.getProperties(), PropertyNames.SERIALIZED_FN)),
                "DoFnInfo");
    assertThat(fnInfo.getDoFn(), instanceOf(TestSplittableFn.class));
    assertThat(
        fnInfo.getWindowingStrategy().getWindowFn(),
        Matchers.<WindowFn>equalTo(FixedWindows.of(Duration.standardMinutes(1))));
    assertThat(fnInfo.getInputCoder(), instanceOf(StringUtf8Coder.class));
    Coder<?> restrictionCoder =
        CloudObjects.coderFromCloudObject(
            (CloudObject)
                Structs.getObject(
                    processKeyedStep.getProperties(), PropertyNames.RESTRICTION_CODER));

    assertEquals(
        KvCoder.of(SerializableCoder.of(OffsetRange.class), VoidCoder.of()), restrictionCoder);
  }

  @Test
  public void testPortablePipelineContainsExpectedDependenciesAndCapabilities() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(Arrays.asList("beam_fn_api"));
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Impulse.create())
        .apply(
            MapElements.via(
                new SimpleFunction<byte[], String>() {
                  @Override
                  public String apply(byte[] input) {
                    return "";
                  }
                }))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    runner.replaceV1Transforms(pipeline);

    File file1 = File.createTempFile("file1-", ".txt");
    file1.deleteOnExit();
    File file2 = File.createTempFile("file2-", ".txt");
    file2.deleteOnExit();
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(
        Environments.createDockerEnvironment(DataflowRunner.getContainerImageForJob(options))
            .toBuilder()
            .addAllDependencies(
                Environments.getArtifacts(
                    ImmutableList.of("file1.txt=" + file1, "file2.txt=" + file2)))
            .addAllCapabilities(Environments.getJavaCapabilities())
            .build());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);

    JobSpecification result =
        translator.translate(
            pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList());

    Components componentsProto = result.getPipelineProto().getComponents();
    assertThat(
        Iterables.getOnlyElement(componentsProto.getEnvironmentsMap().values())
            .getCapabilitiesList(),
        containsInAnyOrder(Environments.getJavaCapabilities().toArray(new String[0])));
    assertThat(
        Iterables.getOnlyElement(componentsProto.getEnvironmentsMap().values())
            .getDependenciesList(),
        containsInAnyOrder(
            Environments.getArtifacts(ImmutableList.of("file1.txt=" + file1, "file2.txt=" + file2))
                .toArray(new ArtifactInformation[0])));
  }

  @Test
  public void testToSingletonTranslationWithIsmSideInput() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<T> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1)).apply(View.asSingleton());
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();
    assertAllStepOutputsHaveUniqueIds(job);

    List<Step> steps = job.getSteps();
    assertEquals(10, steps.size());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> toIsmRecordOutputs =
        (List<Map<String, Object>>)
            steps.get(steps.size() - 2).getProperties().get(PropertyNames.OUTPUT_INFO);
    assertTrue(
        Structs.getBoolean(Iterables.getOnlyElement(toIsmRecordOutputs), "use_indexed_format"));

    Step collectionToSingletonStep = steps.get(steps.size() - 1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());
  }

  @Test
  public void testToIterableTranslationWithIsmSideInput() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<Iterable<T>> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(View.asIterable());

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();
    assertAllStepOutputsHaveUniqueIds(job);

    List<Step> steps = job.getSteps();
    assertEquals(3, steps.size());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> toIsmRecordOutputs =
        (List<Map<String, Object>>)
            steps.get(steps.size() - 2).getProperties().get(PropertyNames.OUTPUT_INFO);
    assertTrue(
        Structs.getBoolean(Iterables.getOnlyElement(toIsmRecordOutputs), "use_indexed_format"));

    Step collectionToSingletonStep = steps.get(steps.size() - 1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());
  }

  private JobSpecification runBatchGroupIntoBatchesAndGetJobSpec(
      Boolean withShardedKey, List<String> experiments) throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(experiments);
    options.setStreaming(false);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<Integer, String>> input =
        pipeline.apply(Create.of(Arrays.asList(KV.of(1, "1"), KV.of(2, "2"), KV.of(3, "3"))));
    if (withShardedKey) {
      input.apply(GroupIntoBatches.<Integer, String>ofSize(3).withShardedKey());
    } else {
      input.apply(GroupIntoBatches.ofSize(3));
    }

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    return translator.translate(
        pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList());
  }

  private JobSpecification runStreamingGroupIntoBatchesAndGetJobSpec(
      Boolean withShardedKey, List<String> experiments) throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(experiments);
    options.setStreaming(true);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<Integer, String>> input =
        pipeline.apply(Create.of(Arrays.asList(KV.of(1, "1"), KV.of(2, "2"), KV.of(3, "3"))));
    if (withShardedKey) {
      input.apply(GroupIntoBatches.<Integer, String>ofSize(3).withShardedKey());
    } else {
      input.apply(GroupIntoBatches.ofSize(3));
    }

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    return translator.translate(
        pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList());
  }

  @Test
  public void testBatchGroupIntoBatchesTranslation() throws Exception {
    JobSpecification jobSpec =
        runBatchGroupIntoBatchesAndGetJobSpec(false, Collections.emptyList());
    List<Step> steps = jobSpec.getJob().getSteps();
    Step shardedStateStep = steps.get(steps.size() - 1);
    Map<String, Object> properties = shardedStateStep.getProperties();
    assertTrue(properties.containsKey(PropertyNames.PRESERVES_KEYS));
    assertEquals("true", getString(properties, PropertyNames.PRESERVES_KEYS));
  }

  @Test
  public void testBatchGroupIntoBatchesWithShardedKeyTranslation() throws Exception {
    List<String> experiments = Collections.emptyList();
    JobSpecification jobSpec = runBatchGroupIntoBatchesAndGetJobSpec(true, experiments);
    List<Step> steps = jobSpec.getJob().getSteps();
    Step shardedStateStep = steps.get(steps.size() - 1);
    Map<String, Object> properties = shardedStateStep.getProperties();
    assertTrue(properties.containsKey(PropertyNames.PRESERVES_KEYS));
    assertEquals("true", getString(properties, PropertyNames.PRESERVES_KEYS));
  }

  @Test
  public void testStreamingGroupIntoBatchesTranslation() throws Exception {
    List<String> experiments =
        new ArrayList<>(
            ImmutableList.of(
                GcpOptions.STREAMING_ENGINE_EXPERIMENT, GcpOptions.WINDMILL_SERVICE_EXPERIMENT));
    JobSpecification jobSpec = runStreamingGroupIntoBatchesAndGetJobSpec(false, experiments);
    List<Step> steps = jobSpec.getJob().getSteps();
    Step shardedStateStep = steps.get(steps.size() - 1);
    Map<String, Object> properties = shardedStateStep.getProperties();
    assertTrue(properties.containsKey(PropertyNames.USES_KEYED_STATE));
    assertEquals("true", getString(properties, PropertyNames.USES_KEYED_STATE));
    assertFalse(properties.containsKey(PropertyNames.ALLOWS_SHARDABLE_STATE));
    assertTrue(properties.containsKey(PropertyNames.PRESERVES_KEYS));
  }

  @Test
  public void testStreamingGroupIntoBatchesWithShardedKeyTranslation() throws Exception {
    List<String> experiments =
        new ArrayList<>(
            ImmutableList.of(
                GcpOptions.STREAMING_ENGINE_EXPERIMENT, GcpOptions.WINDMILL_SERVICE_EXPERIMENT));
    JobSpecification jobSpec = runStreamingGroupIntoBatchesAndGetJobSpec(true, experiments);
    List<Step> steps = jobSpec.getJob().getSteps();
    Step shardedStateStep = steps.get(steps.size() - 1);
    Map<String, Object> properties = shardedStateStep.getProperties();
    assertTrue(properties.containsKey(PropertyNames.USES_KEYED_STATE));
    assertEquals("true", getString(properties, PropertyNames.USES_KEYED_STATE));
    assertTrue(properties.containsKey(PropertyNames.ALLOWS_SHARDABLE_STATE));
    assertEquals("true", getString(properties, PropertyNames.ALLOWS_SHARDABLE_STATE));
    assertTrue(properties.containsKey(PropertyNames.PRESERVES_KEYS));
    assertEquals("true", getString(properties, PropertyNames.PRESERVES_KEYS));
  }

  @Test
  public void testGroupIntoBatchesWithShardedKeyNotSupported() throws IOException {
    // Not using streaming engine.
    List<String> experiments = new ArrayList<>(ImmutableList.of("use_runner_v2"));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Runner determined sharding not available in Dataflow for GroupIntoBatches for non-Streaming-Engine jobs");
    runStreamingGroupIntoBatchesAndGetJobSpec(true, experiments);
  }

  @Test
  public void testStepDisplayData() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    DoFn<Integer, Integer> fn1 =
        new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(c.element());
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder
                .add(DisplayData.item("foo", "bar"))
                .add(
                    DisplayData.item("foo2", DataflowPipelineTranslatorTest.class)
                        .withLabel("Test Class")
                        .withLinkUrl("http://www.google.com"));
          }
        };

    DoFn<Integer, Integer> fn2 =
        new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(c.element());
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo3", 1234));
          }
        };

    ParDo.SingleOutput<Integer, Integer> parDo1 = ParDo.of(fn1);
    ParDo.SingleOutput<Integer, Integer> parDo2 = ParDo.of(fn2);
    pipeline.apply(Create.of(1, 2, 3)).apply(parDo1).apply(parDo2);

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();
    assertAllStepOutputsHaveUniqueIds(job);

    List<Step> steps = job.getSteps();
    assertEquals(3, steps.size());

    Map<String, Object> parDo1Properties = steps.get(1).getProperties();
    Map<String, Object> parDo2Properties = steps.get(2).getProperties();
    assertThat(parDo1Properties, hasKey("display_data"));

    @SuppressWarnings("unchecked")
    Collection<Map<String, String>> fn1displayData =
        (Collection<Map<String, String>>) parDo1Properties.get("display_data");
    @SuppressWarnings("unchecked")
    Collection<Map<String, String>> fn2displayData =
        (Collection<Map<String, String>>) parDo2Properties.get("display_data");

    ImmutableSet<ImmutableMap<String, Object>> expectedFn1DisplayData =
        ImmutableSet.of(
            ImmutableMap.<String, Object>builder()
                .put("key", "foo")
                .put("type", "STRING")
                .put("value", "bar")
                .put("namespace", fn1.getClass().getName())
                .build(),
            ImmutableMap.<String, Object>builder()
                .put("key", "fn")
                .put("label", "Transform Function")
                .put("type", "JAVA_CLASS")
                .put("value", fn1.getClass().getName())
                .put("shortValue", fn1.getClass().getSimpleName())
                .put("namespace", parDo1.getClass().getName())
                .build(),
            ImmutableMap.<String, Object>builder()
                .put("key", "foo2")
                .put("type", "JAVA_CLASS")
                .put("value", DataflowPipelineTranslatorTest.class.getName())
                .put("shortValue", DataflowPipelineTranslatorTest.class.getSimpleName())
                .put("namespace", fn1.getClass().getName())
                .put("label", "Test Class")
                .put("linkUrl", "http://www.google.com")
                .build());

    ImmutableSet<ImmutableMap<String, Object>> expectedFn2DisplayData =
        ImmutableSet.of(
            ImmutableMap.<String, Object>builder()
                .put("key", "fn")
                .put("label", "Transform Function")
                .put("type", "JAVA_CLASS")
                .put("value", fn2.getClass().getName())
                .put("shortValue", fn2.getClass().getSimpleName())
                .put("namespace", parDo2.getClass().getName())
                .build(),
            ImmutableMap.<String, Object>builder()
                .put("key", "foo3")
                .put("type", "INTEGER")
                .put("value", 1234L)
                .put("namespace", fn2.getClass().getName())
                .build());

    assertEquals(expectedFn1DisplayData, ImmutableSet.copyOf(fn1displayData));
    assertEquals(expectedFn2DisplayData, ImmutableSet.copyOf(fn2displayData));
  }

  @Test
  public void testStepResourceHints() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(
            "Has hints",
            MapElements.into(TypeDescriptors.integers())
                .via((Integer x) -> x + 1)
                .setResourceHints(
                    ResourceHints.create()
                        .withMinRam("10.0GiB")
                        .withAccelerator("type:nvidia-tesla-k80;count:1;install-nvidia-driver")));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceV1Transforms(pipeline);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);
    Job job =
        translator
            .translate(pipeline, pipelineProto, sdkComponents, runner, Collections.emptyList())
            .getJob();

    Step stepWithHints = job.getSteps().get(1);
    ImmutableMap<String, Object> expectedHints =
        ImmutableMap.<String, Object>builder()
            .put("beam:resources:min_ram_bytes:v1", "10737418240")
            .put(
                "beam:resources:accelerator:v1",
                "type:nvidia-tesla-k80;count:1;install-nvidia-driver")
            .build();
    assertEquals(expectedHints, stepWithHints.getProperties().get("resource_hints"));
  }

  private RunnerApi.PTransform getLeafTransform(RunnerApi.Pipeline pipelineProto, String label) {
    for (RunnerApi.PTransform transform :
        pipelineProto.getComponents().getTransformsMap().values()) {
      if (transform.getUniqueName().contains(label) && transform.getSubtransformsCount() == 0) {
        return transform;
      }
    }
    throw new java.lang.IllegalArgumentException(label);
  }

  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T input, OutputReceiver<T> out) {
      out.output(input);
    }
  }

  private static class Inner extends PTransform<PCollection<byte[]>, PCollection<byte[]>> {
    @Override
    public PCollection<byte[]> expand(PCollection<byte[]> input) {
      return input.apply(
          "Innermost",
          ParDo.of(new IdentityDoFn<byte[]>())
              .setResourceHints(ResourceHints.create().withAccelerator("set_in_inner_transform")));
    }
  }

  private static class Outer extends PTransform<PCollection<byte[]>, PCollection<byte[]>> {
    @Override
    public PCollection<byte[]> expand(PCollection<byte[]> input) {
      return input.apply(new Inner());
    }
  }

  @Test
  public void testResourceHintsTranslationsResolvesHintsOnOptionsAndComposites() {
    ResourceHintsOptions options = PipelineOptionsFactory.as(ResourceHintsOptions.class);
    options.setResourceHints(Arrays.asList("accelerator=set_via_options", "minRam=1B"));
    Pipeline pipeline = Pipeline.create(options);
    PCollection<byte[]> root = pipeline.apply(Impulse.create());
    root.apply(
        new Outer()
            .setResourceHints(
                ResourceHints.create().withAccelerator("set_on_outer_transform").withMinRam(20)));
    root.apply("Leaf", ParDo.of(new IdentityDoFn<byte[]>()));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    assertThat(
        pipelineProto
            .getComponents()
            .getEnvironmentsMap()
            .get(getLeafTransform(pipelineProto, "Leaf").getEnvironmentId())
            .getResourceHintsMap(),
        org.hamcrest.Matchers.allOf(
            org.hamcrest.Matchers.hasEntry(
                "beam:resources:min_ram_bytes:v1", ByteString.copyFromUtf8("1")),
            org.hamcrest.Matchers.hasEntry(
                "beam:resources:accelerator:v1", ByteString.copyFromUtf8("set_via_options"))));
    assertThat(
        pipelineProto
            .getComponents()
            .getEnvironmentsMap()
            .get(getLeafTransform(pipelineProto, "Innermost").getEnvironmentId())
            .getResourceHintsMap(),
        org.hamcrest.Matchers.allOf(
            org.hamcrest.Matchers.hasEntry(
                "beam:resources:min_ram_bytes:v1", ByteString.copyFromUtf8("20")),
            org.hamcrest.Matchers.hasEntry(
                "beam:resources:accelerator:v1",
                ByteString.copyFromUtf8("set_in_inner_transform"))));
  }

  /**
   * Tests that when (deprecated) {@link
   * DataflowPipelineOptions#setWorkerHarnessContainerImage(String)} pipeline option is set, {@link
   * DataflowRunner} sets that value as the {@link DockerPayload#getContainerImage()} of the default
   * {@link Environment} used when generating the model pipeline proto.
   */
  @Test
  public void testSetWorkerHarnessContainerImageInPipelineProto() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    String containerImage = "gcr.io/image:foo";
    options.as(DataflowPipelineOptions.class).setWorkerHarnessContainerImage(containerImage);

    Pipeline p = Pipeline.create(options);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p, sdkComponents, true);
    JobSpecification specification =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                proto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList());
    RunnerApi.Pipeline pipelineProto = specification.getPipelineProto();

    assertEquals(1, pipelineProto.getComponents().getEnvironmentsCount());
    Environment defaultEnvironment =
        Iterables.getOnlyElement(pipelineProto.getComponents().getEnvironmentsMap().values());

    DockerPayload payload = DockerPayload.parseFrom(defaultEnvironment.getPayload());
    assertEquals(DataflowRunner.getContainerImageForJob(options), payload.getContainerImage());
  }

  /**
   * Tests that when {@link DataflowPipelineOptions#setSdkContainerImage(String)} pipeline option is
   * set, {@link DataflowRunner} sets that value as the {@link DockerPayload#getContainerImage()} of
   * the default {@link Environment} used when generating the model pipeline proto.
   */
  @Test
  public void testSetSdkContainerImageInPipelineProto() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    String containerImage = "gcr.io/image:foo";
    options.as(DataflowPipelineOptions.class).setSdkContainerImage(containerImage);

    Pipeline p = Pipeline.create(options);
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p, sdkComponents, true);
    JobSpecification specification =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                proto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList());
    RunnerApi.Pipeline pipelineProto = specification.getPipelineProto();

    assertEquals(1, pipelineProto.getComponents().getEnvironmentsCount());
    Environment defaultEnvironment =
        Iterables.getOnlyElement(pipelineProto.getComponents().getEnvironmentsMap().values());

    DockerPayload payload = DockerPayload.parseFrom(defaultEnvironment.getPayload());
    assertEquals(DataflowRunner.getContainerImageForJob(options), payload.getContainerImage());
  }

  @Test
  public void testDataflowServiceOptionsSet() throws IOException {
    final List<String> dataflowServiceOptions =
        Stream.of("whizz=bang", "foo=bar").collect(Collectors.toList());

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setDataflowServiceOptions(dataflowServiceOptions);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertEquals(dataflowServiceOptions, job.getEnvironment().getServiceOptions());
  }

  @Test
  public void testHotKeyLoggingEnabledOption() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setHotKeyLoggingEnabled(true);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    SdkComponents sdkComponents = createSdkComponents(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p,
                pipelineProto,
                sdkComponents,
                DataflowRunner.fromOptions(options),
                Collections.emptyList())
            .getJob();

    assertTrue(job.getEnvironment().getDebugOptions().getEnableHotKeyLogging());
  }

  private static void assertAllStepOutputsHaveUniqueIds(Job job) throws Exception {
    List<String> outputIds = new ArrayList<>();
    for (Step step : job.getSteps()) {
      List<Map<String, Object>> outputInfoList =
          (List<Map<String, Object>>) step.getProperties().get(PropertyNames.OUTPUT_INFO);
      if (outputInfoList != null) {
        for (Map<String, Object> outputInfo : outputInfoList) {
          outputIds.add(getString(outputInfo, PropertyNames.OUTPUT_NAME));
        }
      }
    }
    Set<String> uniqueOutputNames = new HashSet<>(outputIds);
    outputIds.removeAll(uniqueOutputNames);
    assertTrue(String.format("Found duplicate output ids %s", outputIds), outputIds.isEmpty());
  }

  private static class TestSplittableFn extends DoFn<String, Integer> {

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      // noop
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element String element) {
      return null;
    }
  }
}
