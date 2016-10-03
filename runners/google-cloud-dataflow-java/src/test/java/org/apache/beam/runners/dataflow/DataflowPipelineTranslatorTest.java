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

import static org.apache.beam.sdk.util.Structs.addObject;
import static org.apache.beam.sdk.util.Structs.getDictionary;
import static org.apache.beam.sdk.util.Structs.getString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.Step;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.OutputReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.Structs;
import org.apache.beam.sdk.util.TestCredential;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for DataflowPipelineTranslator.
 */
@RunWith(JUnit4.class)
public class DataflowPipelineTranslatorTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  // A Custom Mockito matcher for an initial Job that checks that all
  // expected fields are set.
  private static class IsValidCreateRequest extends ArgumentMatcher<Job> {
    @Override
    public boolean matches(Object o) {
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

    p.apply("ReadMyFile", TextIO.Read.from("gs://bucket/object"))
     .apply("WriteMyFile", TextIO.Write.to("gs://bucket/object"));

    return p;
  }

  private static Dataflow buildMockDataflow(
      ArgumentMatcher<Job> jobMatcher) throws IOException {
    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Jobs mockJobs = mock(Dataflow.Projects.Jobs.class);
    Dataflow.Projects.Jobs.Create mockRequest = mock(
        Dataflow.Projects.Jobs.Create.class);

    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.jobs()).thenReturn(mockJobs);
    when(mockJobs.create(eq("someProject"), argThat(jobMatcher)))
        .thenReturn(mockRequest);

    Job resultJob = new Job();
    resultJob.setId("newid");
    when(mockRequest.execute()).thenReturn(resultJob);
    return mockDataflowClient;
  }

  private static DataflowPipelineOptions buildPipelineOptions() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.expand(any(GcsPath.class))).then(new Answer<List<GcsPath>>() {
      @Override
      public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
      }
    });
    when(mockGcsUtil.bucketAccessible(any(GcsPath.class))).thenReturn(true);
    when(mockGcsUtil.isGcsPatternSupported(anyString())).thenCallRealMethod();

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setJobName("some-job-name");
    options.setProject("some-project");
    options.setTempLocation(GcsPath.fromComponents("somebucket", "some/path").toString());
    options.setFilesToStage(new LinkedList<String>());
    options.setDataflowClient(buildMockDataflow(new IsValidCreateRequest()));
    options.setGcsUtil(mockGcsUtil);
    return options;
  }

  @Test
  public void testSettingOfSdkPipelineOptions() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowRunner.class);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    // Note that the contents of this materialized map may be changed by the act of reading an
    // option, which will cause the default to get materialized whereas it would otherwise be
    // left absent. It is permissible to simply alter this test to reflect current behavior.
    Map<String, Object> settings = new HashMap<>();
    settings.put("appName", "DataflowPipelineTranslatorTest");
    settings.put("project", "some-project");
    settings.put("pathValidatorClass",
        "org.apache.beam.sdk.util.GcsPathValidator");
    settings.put("runner", "org.apache.beam.runners.dataflow.DataflowRunner");
    settings.put("jobName", "some-job-name");
    settings.put("tempLocation", "gs://somebucket/some/path");
    settings.put("gcpTempLocation", "gs://somebucket/some/path");
    settings.put("stagingLocation", "gs://somebucket/some/path/staging");
    settings.put("stableUniqueNames", "WARNING");
    settings.put("streaming", false);
    settings.put("numberOfWorkerHarnessThreads", 0);
    settings.put("experiments", null);

    Map<String, Object> sdkPipelineOptions = job.getEnvironment().getSdkPipelineOptions();
    assertThat(sdkPipelineOptions, hasKey("options"));
    assertEquals(settings, sdkPipelineOptions.get("options"));
  }

  @Test
  public void testNetworkConfig() throws IOException {
    final String testNetwork = "test-network";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setNetwork(testNetwork);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(testNetwork,
        job.getEnvironment().getWorkerPools().get(0).getNetwork());
  }

  @Test
  public void testNetworkConfigMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
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
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(testSubnetwork,
        job.getEnvironment().getWorkerPools().get(0).getSubnetwork());
  }

  @Test
  public void testSubnetworkConfigMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(job.getEnvironment().getWorkerPools().get(0).getSubnetwork());
  }

  @Test
  public void testScalingAlgorithmMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    // Autoscaling settings are always set.
    assertNull(
        job
            .getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getAlgorithm());
    assertEquals(
        0,
        job
            .getEnvironment()
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

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(
        "AUTOSCALING_ALGORITHM_NONE",
        job
            .getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getAlgorithm());
    assertEquals(
        0,
        job
            .getEnvironment()
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
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(
        job
            .getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getAlgorithm());
    assertEquals(
        42,
        job
            .getEnvironment()
            .getWorkerPools()
            .get(0)
            .getAutoscalingSettings()
            .getMaxNumWorkers()
            .intValue());
  }

  @Test
  public void testZoneConfig() throws IOException {
    final String testZone = "test-zone-1";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setZone(testZone);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(testZone,
        job.getEnvironment().getWorkerPools().get(0).getZone());
  }

  @Test
  public void testWorkerMachineTypeConfig() throws IOException {
    final String testMachineType = "test-machine-type";

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setWorkerMachineType(testMachineType);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
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
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(
                p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertEquals(diskSizeGb,
        job.getEnvironment().getWorkerPools().get(0).getDiskSizeGb());
  }

  @Test
  public void testPredefinedAddStep() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();

    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    DataflowPipelineTranslator.registerTransformTranslator(
        EmbeddedTransform.class, new EmbeddedTranslator());

    // Create a predefined step using another pipeline
    Step predefinedStep = createPredefinedStep();

    // Create a pipeline that the predefined step will be embedded into
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("ReadMyFile", TextIO.Read.from("gs://bucket/in"))
        .apply(ParDo.of(new NoOpFn()))
        .apply(new EmbeddedTransform(predefinedStep.clone()))
        .apply(ParDo.of(new NoOpFn()));
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    List<Step> steps = job.getSteps();
    assertEquals(4, steps.size());

    // The input to the embedded step should match the output of the step before
    Map<String, Object> step1Out = getOutputPortReference(steps.get(1));
    Map<String, Object> step2In = getDictionary(
        steps.get(2).getProperties(), PropertyNames.PARALLEL_INPUT);
    assertEquals(step1Out, step2In);

    // The output from the embedded step should match the input of the step after
    Map<String, Object> step2Out = getOutputPortReference(steps.get(2));
    Map<String, Object> step3In = getDictionary(
        steps.get(3).getProperties(), PropertyNames.PARALLEL_INPUT);
    assertEquals(step2Out, step3In);

    // The step should not have been modified other than remapping the input
    Step predefinedStepClone = predefinedStep.clone();
    Step embeddedStepClone = steps.get(2).clone();
    predefinedStepClone.getProperties().remove(PropertyNames.PARALLEL_INPUT);
    embeddedStepClone.getProperties().remove(PropertyNames.PARALLEL_INPUT);
    assertEquals(predefinedStepClone, embeddedStepClone);
  }

  /**
   * Construct a OutputReference for the output of the step.
   */
  private static OutputReference getOutputPortReference(Step step) throws Exception {
    // TODO: This should be done via a Structs accessor.
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> output =
        (List<Map<String, Object>>) step.getProperties().get(PropertyNames.OUTPUT_INFO);
    String outputTagId = getString(Iterables.getOnlyElement(output), PropertyNames.OUTPUT_NAME);
    return new OutputReference(step.getName(), outputTagId);
  }

  /**
   * Returns a Step for a OldDoFn by creating and translating a pipeline.
   */
  private static Step createPredefinedStep() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline pipeline = Pipeline.create(options);
    String stepName = "DoFn1";
    pipeline.apply("ReadMyFile", TextIO.Read.from("gs://bucket/in"))
        .apply(stepName, ParDo.of(new NoOpFn()))
        .apply("WriteMyFile", TextIO.Write.to("gs://bucket/out"));
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(13, job.getSteps().size());
    Step step = job.getSteps().get(1);
    assertEquals(stepName, getString(step.getProperties(), PropertyNames.USER_NAME));
    return step;
  }

  private static class NoOpFn extends OldDoFn<String, String> {
    @Override public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  /**
   * A placeholder transform that will be used to substitute a predefined Step.
   */
  private static class EmbeddedTransform
      extends PTransform<PCollection<String>, PCollection<String>> {
    private final Step step;

    public EmbeddedTransform(Step step) {
      this.step = step;
    }

    @Override
    public PCollection<String> apply(PCollection<String> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          input.isBounded());
    }

    @Override
    protected Coder<?> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  /**
   * A TransformTranslator that adds the predefined Step using
   * {@link TranslationContext#addStep} and remaps the input port reference.
   */
  private static class EmbeddedTranslator
      implements DataflowPipelineTranslator.TransformTranslator<EmbeddedTransform> {
    @Override public void translate(EmbeddedTransform transform, TranslationContext context) {
      addObject(transform.step.getProperties(), PropertyNames.PARALLEL_INPUT,
          context.asOutputReference(context.getInput(transform)));
      context.addStep(transform, transform.step);
    }
  }

  /**
   * A composite transform that returns an output that is unrelated to
   * the input.
   */
  private static class UnrelatedOutputCreator
      extends PTransform<PCollection<Integer>, PCollection<Integer>> {

    @Override
    public PCollection<Integer> apply(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.<Integer>perElement());

      // Return a value unrelated to the input.
      return input.getPipeline().apply(Create.of(1, 2, 3, 4));
    }

    @Override
    protected Coder<?> getDefaultOutputCoder() {
      return VarIntCoder.of();
    }
  }

  /**
   * A composite transform that returns an output that is unbound.
   */
  private static class UnboundOutputCreator
      extends PTransform<PCollection<Integer>, PDone> {

    @Override
    public PDone apply(PCollection<Integer> input) {
      // Apply an operation so that this is a composite transform.
      input.apply(Count.<Integer>perElement());

      return PDone.in(input.getPipeline());
    }

    @Override
    protected Coder<?> getDefaultOutputCoder() {
      return VoidCoder.of();
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
    public PCollectionTuple apply(PCollection<Integer> input) {
      PCollection<Integer> sum = input.apply(Sum.integersGlobally());

      // Fails here when attempting to construct a tuple with an unbound object.
      return PCollectionTuple.of(sumTag, sum)
          .and(doneTag, PCollection.<Void>createPrimitiveOutputInternal(
              input.getPipeline(),
              WindowingStrategy.globalDefault(),
              input.isBounded()));
    }
  }

  @Test
  public void testMultiGraphPipelineSerialization() throws IOException {
    Pipeline p = Pipeline.create(buildPipelineOptions());

    PCollection<Integer> input = p.begin()
        .apply(Create.of(1, 2, 3));

    input.apply(new UnrelatedOutputCreator());
    input.apply(new UnboundOutputCreator());

    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(
        PipelineOptionsFactory.as(DataflowPipelineOptions.class));

    // Check that translation doesn't fail.
    t.translate(
        p, (DataflowRunner) p.getRunner(), Collections.<DataflowPackage>emptyList());
  }

  @Test
  public void testPartiallyBoundFailure() throws IOException {
    Pipeline p = Pipeline.create(buildPipelineOptions());

    PCollection<Integer> input = p.begin()
        .apply(Create.of(1, 2, 3));

    thrown.expect(IllegalStateException.class);
    input.apply(new PartiallyBoundOutputCreator());

    Assert.fail("Failure expected from use of partially bound output");
  }

  /**
   * This tests a few corner cases that should not crash.
   */
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
    t.translate(
        pipeline,
        (DataflowRunner) pipeline.getRunner(),
        Collections.<DataflowPackage>emptyList());
  }

  private void applyRead(Pipeline pipeline, String path) {
    pipeline.apply("Read(" + path + ")", TextIO.Read.from(path));
  }

  /**
   * Recursive wildcards are not supported.
   * This tests "**".
   */
  @Test
  public void testBadWildcardRecursive() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(options);

    pipeline.apply(TextIO.Read.from("gs://bucket/foo**/baz"));

    // Check that translation does fail.
    thrown.expectCause(allOf(
        instanceOf(IllegalArgumentException.class),
        ThrowableMessageMatcher.hasMessage(containsString("Unsupported wildcard usage"))));
    t.translate(
        pipeline,
        (DataflowRunner) pipeline.getRunner(),
        Collections.<DataflowPackage>emptyList());
  }

  @Test
  public void testToSingletonTranslation() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<T> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(ImmutableList.of("disable_ism_side_input"));
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1))
        .apply(View.<Integer>asSingleton());
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    List<Step> steps = job.getSteps();
    assertEquals(2, steps.size());

    Step createStep = steps.get(0);
    assertEquals("ParallelRead", createStep.getKind());

    Step collectionToSingletonStep = steps.get(1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());

  }

  @Test
  public void testToIterableTranslation() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<Iterable<T>> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(ImmutableList.of("disable_ism_side_input"));
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3))
        .apply(View.<Integer>asIterable());
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    List<Step> steps = job.getSteps();
    assertEquals(2, steps.size());

    Step createStep = steps.get(0);
    assertEquals("ParallelRead", createStep.getKind());

    Step collectionToSingletonStep = steps.get(1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());
  }

  @Test
  public void testToSingletonTranslationWithIsmSideInput() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<T> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1))
        .apply(View.<Integer>asSingleton());
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    List<Step> steps = job.getSteps();
    assertEquals(5, steps.size());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> toIsmRecordOutputs =
        (List<Map<String, Object>>) steps.get(3).getProperties().get(PropertyNames.OUTPUT_INFO);
    assertTrue(
        Structs.getBoolean(Iterables.getOnlyElement(toIsmRecordOutputs), "use_indexed_format"));

    Step collectionToSingletonStep = steps.get(4);
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
    pipeline.apply(Create.of(1, 2, 3))
        .apply(View.<Integer>asIterable());
    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

    List<Step> steps = job.getSteps();
    assertEquals(3, steps.size());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> toIsmRecordOutputs =
        (List<Map<String, Object>>) steps.get(1).getProperties().get(PropertyNames.OUTPUT_INFO);
    assertTrue(
        Structs.getBoolean(Iterables.getOnlyElement(toIsmRecordOutputs), "use_indexed_format"));


    Step collectionToSingletonStep = steps.get(2);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());
  }

  @Test
  public void testStepDisplayData() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    OldDoFn<Integer, Integer> fn1 = new OldDoFn<Integer, Integer>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        c.output(c.element());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder
            .add(DisplayData.item("foo", "bar"))
            .add(DisplayData.item("foo2", DataflowPipelineTranslatorTest.class)
                .withLabel("Test Class")
                .withLinkUrl("http://www.google.com"));
      }
    };

    OldDoFn<Integer, Integer> fn2 = new OldDoFn<Integer, Integer>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        c.output(c.element());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo3", 1234));
      }
    };

    ParDo.Bound<Integer, Integer> parDo1 = ParDo.of(fn1);
    ParDo.Bound<Integer, Integer> parDo2 = ParDo.of(fn2);
    pipeline
      .apply(Create.of(1, 2, 3))
      .apply(parDo1)
      .apply(parDo2);

    Job job =
        translator
            .translate(
                pipeline,
                (DataflowRunner) pipeline.getRunner(),
                Collections.<DataflowPackage>emptyList())
            .getJob();

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

    ImmutableSet<ImmutableMap<String, Object>> expectedFn1DisplayData = ImmutableSet.of(
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
            .build()
    );

    ImmutableSet<ImmutableMap<String, Object>> expectedFn2DisplayData = ImmutableSet.of(
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
            .build()
    );

    assertEquals(expectedFn1DisplayData, ImmutableSet.copyOf(fn1displayData));
    assertEquals(expectedFn2DisplayData, ImmutableSet.copyOf(fn2displayData));
  }
}
