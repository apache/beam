/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static com.google.cloud.dataflow.sdk.util.Structs.addObject;
import static com.google.cloud.dataflow.sdk.util.Structs.getDictionary;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.Step;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.OutputReference;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Tests for DataflowPipelineTranslator.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class DataflowPipelineTranslatorTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

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

  private DataflowPipeline buildPipeline(DataflowPipelineOptions options)
      throws IOException {
    DataflowPipeline p = DataflowPipeline.create(options);

    p.apply(TextIO.Read.named("ReadMyFile").from("gs://bucket/object"))
        .apply(TextIO.Write.named("WriteMyFile").to("gs://bucket/object"));

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
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setJobName("some-job-name");
    options.setProject("some-project");
    options.setTempLocation(GcsPath.fromComponents("somebucket", "some/path").toString());
    options.setFilesToStage(new LinkedList<String>());
    options.setDataflowClient(buildMockDataflow(new IsValidCreateRequest()));
    return options;
  }

  @Test
  public void testSettingOfSdkPipelineOptions() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowPipelineRunner.class);

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(p, Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(ImmutableMap.of("options",
        ImmutableMap.builder()
          .put("appName", "DataflowPipelineTranslatorTest")
          .put("project", "some-project")
          .put("pathValidatorClass", "com.google.cloud.dataflow.sdk.util.DataflowPathValidator")
          .put("runner", "com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner")
          .put("jobName", "some-job-name")
          .put("tempLocation", "gs://somebucket/some/path")
          .put("filesToStage", ImmutableList.of())
          .put("stagingLocation", "gs://somebucket/some/path/staging")
          .put("stableUniqueNames", "WARNING")
          .build()),
        job.getEnvironment().getSdkPipelineOptions());
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
            .translate(p, Collections.<DataflowPackage>emptyList())
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
            .translate(p, Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());
    assertNull(job.getEnvironment().getWorkerPools().get(0).getNetwork());
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
            .translate(p, Collections.<DataflowPackage>emptyList())
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
            .translate(p, Collections.<DataflowPackage>emptyList())
            .getJob();

    assertEquals(1, job.getEnvironment().getWorkerPools().size());

    WorkerPool workerPool = job.getEnvironment().getWorkerPools().get(0);
    assertEquals(testMachineType, workerPool.getMachineType());
  }

  @Test
  public void testDebuggerConfig() throws IOException {
    final String cdbgVersion = "test-v1";
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setCdbgVersion(cdbgVersion);
    String expectedConfig = "{\"version\":\"test-v1\"}";

    Pipeline p = buildPipeline(options);
    p.traverseTopologically(new RecordingPipelineVisitor());
    Job job =
        DataflowPipelineTranslator.fromOptions(options)
            .translate(p, Collections.<DataflowPackage>emptyList())
            .getJob();

    for (WorkerPool pool : job.getEnvironment().getWorkerPools()) {
      if ("harness".equals(pool.getKind())) {
        assertEquals(pool.getMetadata().get("debugger"), expectedConfig);
      }
    }
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
            .translate(p, Collections.<DataflowPackage>emptyList())
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
    DataflowPipeline pipeline = DataflowPipeline.create(options);
    pipeline.apply(TextIO.Read.named("ReadMyFile").from("gs://bucket/in"))
        .apply(ParDo.of(new NoOpFn()))
        .apply(new EmbeddedTransform(predefinedStep.clone()))
        .apply(TextIO.Write.named("WriteMyFile").to("gs://bucket/out"));
    Job job = translator.translate(pipeline, Collections.<DataflowPackage>emptyList()).getJob();

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
   * Returns a Step for a DoFn by creating and translating a pipeline.
   */
  private static Step createPredefinedStep() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    DataflowPipeline pipeline = DataflowPipeline.create(options);
    String stepName = "DoFn1";
    pipeline.apply(TextIO.Read.named("ReadMyFile").from("gs://bucket/in"))
        .apply(ParDo.of(new NoOpFn()).named(stepName))
        .apply(TextIO.Write.named("WriteMyFile").to("gs://bucket/out"));
    Job job = translator.translate(pipeline, Collections.<DataflowPackage>emptyList()).getJob();

    assertEquals(3, job.getSteps().size());
    Step step = job.getSteps().get(1);
    assertEquals(stepName, getString(step.getProperties(), PropertyNames.USER_NAME));
    return step;
  }

  private static class NoOpFn extends DoFn<String, String>{
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
   * <p> This is not allowed and will result in a failure.
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
    Pipeline p = DataflowPipeline.create(buildPipelineOptions());

    PCollection<Integer> input = p.begin()
        .apply(Create.of(1, 2, 3));

    input.apply(new UnrelatedOutputCreator());
    input.apply(new UnboundOutputCreator());

    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(
        PipelineOptionsFactory.as(DataflowPipelineOptions.class));

    // Check that translation doesn't fail.
    t.translate(p, Collections.<DataflowPackage>emptyList());
  }

  @Test
  public void testPartiallyBoundFailure() throws IOException {
    Pipeline p = DataflowPipeline.create(buildPipelineOptions());

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
    Pipeline pipeline = DataflowPipeline.create(options);
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
    t.translate(pipeline, Collections.<DataflowPackage>emptyList());
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
    Pipeline pipeline = DataflowPipeline.create(options);
    DataflowPipelineTranslator t = DataflowPipelineTranslator.fromOptions(options);

    pipeline.apply(TextIO.Read.from("gs://bucket/foo**/baz"));

    // Check that translation does fail.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported wildcard usage");
    t.translate(pipeline, Collections.<DataflowPackage>emptyList());
  }

  @Test
  public void testToSingletonTranslation() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<T> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    DataflowPipeline pipeline = DataflowPipeline.create(options);
    pipeline.apply(Create.of(1))
        .apply(View.<Integer>asSingleton());
    Job job = translator.translate(pipeline, Collections.<DataflowPackage>emptyList()).getJob();

    List<Step> steps = job.getSteps();
    assertEquals(2, steps.size());

    Step createStep = steps.get(0);
    assertEquals("CreateCollection", createStep.getKind());

    Step collectionToSingletonStep = steps.get(1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());

  }

  @Test
  public void testToIterableTranslation() throws Exception {
    // A "change detector" test that makes sure the translation
    // of getting a PCollectionView<Iterable<T>> does not change
    // in bad ways during refactor

    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);

    DataflowPipeline pipeline = DataflowPipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3))
        .apply(View.<Integer>asIterable());
    Job job = translator.translate(pipeline, Collections.<DataflowPackage>emptyList()).getJob();

    List<Step> steps = job.getSteps();
    assertEquals(2, steps.size());

    Step createStep = steps.get(0);
    assertEquals("CreateCollection", createStep.getKind());

    Step collectionToSingletonStep = steps.get(1);
    assertEquals("CollectionToSingleton", collectionToSingletonStep.getKind());
  }
}
