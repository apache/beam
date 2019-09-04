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

import static org.apache.beam.runners.dataflow.DataflowRunner.getContainerImageForJob;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.auto.service.AutoService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner.BatchGroupIntoBatches;
import org.apache.beam.runners.dataflow.DataflowRunner.StreamingShardedWriteFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for the {@link DataflowRunner}.
 *
 * <p>Implements {@link Serializable} because it is caught in closures.
 */
@RunWith(JUnit4.class)
public class DataflowRunnerTest implements Serializable {

  private static final String VALID_BUCKET = "valid-bucket";
  private static final String VALID_STAGING_BUCKET = "gs://valid-bucket/staging";
  private static final String VALID_TEMP_BUCKET = "gs://valid-bucket/temp";
  private static final String VALID_PROFILE_BUCKET = "gs://valid-bucket/profiles";
  private static final String NON_EXISTENT_BUCKET = "gs://non-existent-bucket/location";

  private static final String PROJECT_ID = "some-project";
  private static final String REGION_ID = "some-region-1";

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient ExpectedLogs expectedLogs = ExpectedLogs.none(DataflowRunner.class);
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private transient Dataflow.Projects.Locations.Jobs mockJobs;
  private transient GcsUtil mockGcsUtil;

  // Asserts that the given Job has all expected fields set.
  private static void assertValidJob(Job job) {
    assertNull(job.getId());
    assertNull(job.getCurrentState());
    assertTrue(Pattern.matches("[a-z]([-a-z0-9]*[a-z0-9])?", job.getName()));

    assertThat(
        (Map<String, Object>) job.getEnvironment().getSdkPipelineOptions().get("options"),
        hasKey("pipelineUrl"));
  }

  @Before
  public void setUp() throws IOException {
    this.mockGcsUtil = mock(GcsUtil.class);

    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    when(mockGcsUtil.create(any(GcsPath.class), anyString(), anyInt()))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    when(mockGcsUtil.expand(any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri(VALID_STAGING_BUCKET))).thenReturn(true);
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri(VALID_TEMP_BUCKET))).thenReturn(true);
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri(VALID_TEMP_BUCKET + "/staging/")))
        .thenReturn(true);
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri(VALID_PROFILE_BUCKET))).thenReturn(true);
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri(NON_EXISTENT_BUCKET))).thenReturn(false);

    // Let every valid path be matched
    when(mockGcsUtil.getObjects(anyListOf(GcsPath.class)))
        .thenAnswer(
            invocationOnMock -> {
              List<GcsPath> gcsPaths = (List<GcsPath>) invocationOnMock.getArguments()[0];
              List<GcsUtil.StorageObjectOrIOException> results = new ArrayList<>();

              for (GcsPath gcsPath : gcsPaths) {
                if (gcsPath.getBucket().equals(VALID_BUCKET)) {
                  StorageObject resultObject = new StorageObject();
                  resultObject.setBucket(gcsPath.getBucket());
                  resultObject.setName(gcsPath.getObject());
                  results.add(GcsUtil.StorageObjectOrIOException.create(resultObject));
                }
              }

              return results;
            });

    // The dataflow pipeline attempts to output to this location.
    when(mockGcsUtil.bucketAccessible(GcsPath.fromUri("gs://bucket/object"))).thenReturn(true);

    mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);
  }

  private Pipeline buildDataflowPipeline(DataflowPipelineOptions options) {
    options.setStableUniqueNames(CheckEnabled.ERROR);
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadMyFile", TextIO.read().from("gs://bucket/object"))
        .apply("WriteMyFile", TextIO.write().to("gs://bucket/object"));

    // Enable the FileSystems API to know about gs:// URIs in this test.
    FileSystems.setDefaultPipelineOptions(options);

    return p;
  }

  private Dataflow buildMockDataflow() throws IOException {
    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Locations mockLocations = mock(Dataflow.Projects.Locations.class);
    Dataflow.Projects.Locations.Jobs.Create mockRequest =
        mock(Dataflow.Projects.Locations.Jobs.Create.class);
    Dataflow.Projects.Locations.Jobs.List mockList =
        mock(Dataflow.Projects.Locations.Jobs.List.class);

    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.jobs()).thenReturn(mockJobs);
    when(mockJobs.create(eq(PROJECT_ID), eq(REGION_ID), isA(Job.class))).thenReturn(mockRequest);
    when(mockJobs.list(eq(PROJECT_ID), eq(REGION_ID))).thenReturn(mockList);
    when(mockList.setPageToken(any())).thenReturn(mockList);
    when(mockList.execute())
        .thenReturn(
            new ListJobsResponse()
                .setJobs(
                    Arrays.asList(
                        new Job()
                            .setName("oldjobname")
                            .setId("oldJobId")
                            .setCurrentState("JOB_STATE_RUNNING"))));

    Job resultJob = new Job();
    resultJob.setId("newid");
    when(mockRequest.execute()).thenReturn(resultJob);
    return mockDataflowClient;
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DELETE_ON_CLOSE));
    when(mockGcsUtil.expand(any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));
    return mockGcsUtil;
  }

  private DataflowPipelineOptions buildPipelineOptions() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject(PROJECT_ID);
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setRegion(REGION_ID);
    // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
    options.setFilesToStage(new ArrayList<>());
    options.setDataflowClient(buildMockDataflow());
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    // Configure the FileSystem registrar to use these options.
    FileSystems.setDefaultPipelineOptions(options);

    return options;
  }

  @Test
  public void testPathValidation() {
    String[] args =
        new String[] {
          "--runner=DataflowRunner",
          "--tempLocation=/tmp/not/a/gs/path",
          "--project=test-project",
          "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
        };

    try {
      Pipeline.create(PipelineOptionsFactory.fromArgs(args).create()).run();
      fail();
    } catch (RuntimeException e) {
      assertThat(
          Throwables.getStackTraceAsString(e),
          containsString("DataflowRunner requires gcpTempLocation"));
    }
  }

  @Test
  public void testPathExistsValidation() {
    String[] args =
        new String[] {
          "--runner=DataflowRunner",
          "--tempLocation=gs://does/not/exist",
          "--project=test-project",
          "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
        };

    try {
      Pipeline.create(PipelineOptionsFactory.fromArgs(args).create()).run();
      fail();
    } catch (RuntimeException e) {
      assertThat(
          Throwables.getStackTraceAsString(e),
          both(containsString("gs://does/not/exist"))
              .and(containsString("Unable to verify that GCS bucket gs://does exists")));
    }
  }

  @Test
  public void testPathValidatorOverride() {
    String[] args =
        new String[] {
          "--runner=DataflowRunner",
          "--tempLocation=/tmp/testing",
          "--project=test-project",
          "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
          "--pathValidatorClass=" + NoopPathValidator.class.getName(),
        };
    // Should not crash, because gcpTempLocation should get set from tempLocation
    TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
  }

  @Test
  public void testFromOptionsWithUppercaseConvertsToLowercase() throws Exception {
    String mixedCase = "ThisJobNameHasMixedCase";
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setJobName(mixedCase);

    DataflowRunner.fromOptions(options);
    assertThat(options.getJobName(), equalTo(mixedCase.toLowerCase()));
  }

  @Test
  public void testFromOptionsUserAgentFromPipelineInfo() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner.fromOptions(options);

    String expectedName = DataflowRunnerInfo.getDataflowRunnerInfo().getName().replace(" ", "_");
    assertThat(options.getUserAgent(), containsString(expectedName));

    String expectedVersion = DataflowRunnerInfo.getDataflowRunnerInfo().getVersion();
    assertThat(options.getUserAgent(), containsString(expectedVersion));
  }

  /**
   * Invasive mock-based test for checking that the JSON generated for the pipeline options has not
   * had vital fields pruned.
   */
  @Test
  public void testSettingOfSdkPipelineOptions() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

    // These options are important only for this test, and need not be global to the test class
    options.setAppName(DataflowRunnerTest.class.getSimpleName());
    options.setJobName("some-job-name");

    Pipeline p = Pipeline.create(options);
    p.run();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());

    Map<String, Object> sdkPipelineOptions =
        jobCaptor.getValue().getEnvironment().getSdkPipelineOptions();

    assertThat(sdkPipelineOptions, hasKey("options"));
    Map<String, Object> optionsMap = (Map<String, Object>) sdkPipelineOptions.get("options");

    assertThat(optionsMap, hasEntry("appName", (Object) options.getAppName()));
    assertThat(optionsMap, hasEntry("project", (Object) options.getProject()));
    assertThat(
        optionsMap,
        hasEntry("pathValidatorClass", (Object) options.getPathValidatorClass().getName()));
    assertThat(optionsMap, hasEntry("runner", (Object) options.getRunner().getName()));
    assertThat(optionsMap, hasEntry("jobName", (Object) options.getJobName()));
    assertThat(optionsMap, hasEntry("tempLocation", (Object) options.getTempLocation()));
    assertThat(optionsMap, hasEntry("stagingLocation", (Object) options.getStagingLocation()));
    assertThat(
        optionsMap,
        hasEntry("stableUniqueNames", (Object) options.getStableUniqueNames().toString()));
    assertThat(optionsMap, hasEntry("streaming", (Object) options.isStreaming()));
    assertThat(
        optionsMap,
        hasEntry(
            "numberOfWorkerHarnessThreads", (Object) options.getNumberOfWorkerHarnessThreads()));
  }

  @Test
  public void testSettingFlexRS() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setFlexRSGoal(DataflowPipelineOptions.FlexResourceSchedulingGoal.COST_OPTIMIZED);

    Pipeline p = Pipeline.create(options);
    p.run();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());

    assertEquals(
        "FLEXRS_COST_OPTIMIZED",
        jobCaptor.getValue().getEnvironment().getFlexResourceSchedulingGoal());
  }

  /** PipelineOptions used to test auto registration of Jackson modules. */
  public interface JacksonIncompatibleOptions extends PipelineOptions {
    JacksonIncompatible getJacksonIncompatible();

    void setJacksonIncompatible(JacksonIncompatible value);
  }

  /** A Jackson {@link Module} to test auto-registration of modules. */
  @AutoService(Module.class)
  public static class RegisteredTestModule extends SimpleModule {
    public RegisteredTestModule() {
      super("RegisteredTestModule");
      setMixInAnnotation(JacksonIncompatible.class, JacksonIncompatibleMixin.class);
    }
  }

  /** A class which Jackson does not know how to serialize/deserialize. */
  public static class JacksonIncompatible {
    private final String value;

    public JacksonIncompatible(String value) {
      this.value = value;
    }
  }

  /** A Jackson mixin used to add annotations to other classes. */
  @JsonDeserialize(using = JacksonIncompatibleDeserializer.class)
  @JsonSerialize(using = JacksonIncompatibleSerializer.class)
  public static final class JacksonIncompatibleMixin {}

  /** A Jackson deserializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleDeserializer
      extends JsonDeserializer<JacksonIncompatible> {

    @Override
    public JacksonIncompatible deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return new JacksonIncompatible(jsonParser.readValueAs(String.class));
    }
  }

  /** A Jackson serializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleSerializer extends JsonSerializer<JacksonIncompatible> {

    @Override
    public void serialize(
        JacksonIncompatible jacksonIncompatible,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException, JsonProcessingException {
      jsonGenerator.writeString(jacksonIncompatible.value);
    }
  }

  @Test
  public void testSettingOfPipelineOptionsWithCustomUserType() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options
        .as(JacksonIncompatibleOptions.class)
        .setJacksonIncompatible(new JacksonIncompatible("userCustomTypeTest"));

    Pipeline p = Pipeline.create(options);
    p.run();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());

    Map<String, Object> sdkPipelineOptions =
        jobCaptor.getValue().getEnvironment().getSdkPipelineOptions();
    assertThat(sdkPipelineOptions, hasKey("options"));
    Map<String, Object> optionsMap = (Map<String, Object>) sdkPipelineOptions.get("options");
    assertThat(optionsMap, hasEntry("jacksonIncompatible", (Object) "userCustomTypeTest"));
  }

  @Test
  public void testRun() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  /** Options for testing. */
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getInput();

    void setInput(ValueProvider<String> value);

    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);
  }

  @Test
  public void testTextIOWithRuntimeParameters() throws IOException {
    DataflowPipelineOptions dataflowOptions = buildPipelineOptions();
    RuntimeTestOptions options = dataflowOptions.as(RuntimeTestOptions.class);
    Pipeline p = buildDataflowPipeline(dataflowOptions);
    p.apply(TextIO.read().from(options.getInput())).apply(TextIO.write().to(options.getOutput()));
  }

  /** Tests that all reads are consumed by at least one {@link PTransform}. */
  @Test
  public void testUnconsumedReads() throws IOException {
    DataflowPipelineOptions dataflowOptions = buildPipelineOptions();
    RuntimeTestOptions options = dataflowOptions.as(RuntimeTestOptions.class);
    Pipeline p = buildDataflowPipeline(dataflowOptions);
    p.apply(TextIO.read().from(options.getInput()));
    DataflowRunner.fromOptions(dataflowOptions).replaceTransforms(p);
    final AtomicBoolean unconsumedSeenAsInput = new AtomicBoolean();
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            unconsumedSeenAsInput.set(true);
          }
        });
    assertThat(unconsumedSeenAsInput.get(), is(true));
  }

  @Test
  public void testRunReturnDifferentRequestId() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Dataflow mockDataflowClient = options.getDataflowClient();
    Dataflow.Projects.Locations.Jobs.Create mockRequest =
        mock(Dataflow.Projects.Locations.Jobs.Create.class);
    when(mockDataflowClient
            .projects()
            .locations()
            .jobs()
            .create(eq(PROJECT_ID), eq(REGION_ID), any(Job.class)))
        .thenReturn(mockRequest);
    Job resultJob = new Job();
    resultJob.setId("newid");
    // Return a different request id.
    resultJob.setClientRequestId("different_request_id");
    when(mockRequest.execute()).thenReturn(resultJob);

    Pipeline p = buildDataflowPipeline(options);
    try {
      p.run();
      fail("Expected DataflowJobAlreadyExistsException");
    } catch (DataflowJobAlreadyExistsException expected) {
      assertThat(
          expected.getMessage(),
          containsString(
              "If you want to submit a second job, try again by setting a "
                  + "different name using --jobName."));
      assertEquals(expected.getJob().getJobId(), resultJob.getId());
    }
  }

  @Test
  public void testUpdate() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setUpdate(true);
    options.setJobName("oldJobName");
    Pipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testUploadGraph() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setExperiments(Arrays.asList("upload_graph"));
    Pipeline p = buildDataflowPipeline(options);
    DataflowPipelineJob job = (DataflowPipelineJob) p.run();

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
    assertTrue(jobCaptor.getValue().getSteps().isEmpty());
    assertTrue(
        jobCaptor
            .getValue()
            .getStepsLocation()
            .startsWith("gs://valid-bucket/temp/staging/dataflow_graph"));
  }

  @Test
  public void testUpdateNonExistentPipeline() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Could not find running job named badjobname");

    DataflowPipelineOptions options = buildPipelineOptions();
    options.setUpdate(true);
    options.setJobName("badJobName");
    Pipeline p = buildDataflowPipeline(options);
    p.run();
  }

  @Test
  public void testUpdateAlreadyUpdatedPipeline() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setUpdate(true);
    options.setJobName("oldJobName");
    Dataflow mockDataflowClient = options.getDataflowClient();
    Dataflow.Projects.Locations.Jobs.Create mockRequest =
        mock(Dataflow.Projects.Locations.Jobs.Create.class);
    when(mockDataflowClient
            .projects()
            .locations()
            .jobs()
            .create(eq(PROJECT_ID), eq(REGION_ID), any(Job.class)))
        .thenReturn(mockRequest);
    final Job resultJob = new Job();
    resultJob.setId("newid");
    // Return a different request id.
    resultJob.setClientRequestId("different_request_id");
    when(mockRequest.execute()).thenReturn(resultJob);

    Pipeline p = buildDataflowPipeline(options);

    thrown.expect(DataflowJobAlreadyUpdatedException.class);
    thrown.expect(
        new TypeSafeMatcher<DataflowJobAlreadyUpdatedException>() {
          @Override
          public void describeTo(Description description) {
            description.appendText("Expected job ID: " + resultJob.getId());
          }

          @Override
          protected boolean matchesSafely(DataflowJobAlreadyUpdatedException item) {
            return resultJob.getId().equals(item.getJob().getJobId());
          }
        });
    thrown.expectMessage(
        "The job named oldjobname with id: oldJobId has already been updated "
            + "into job id: newid and cannot be updated again.");
    p.run();
  }

  @Test
  public void testRunWithFiles() throws IOException {
    // Test that the function DataflowRunner.stageFiles works as expected.
    final String cloudDataflowDataset = "somedataset";

    // Create some temporary files.
    File temp1 = File.createTempFile("DataflowRunnerTest", "txt");
    temp1.deleteOnExit();
    File temp2 = File.createTempFile("DataflowRunnerTest2", "txt");
    temp2.deleteOnExit();

    String overridePackageName = "alias.txt";

    when(mockGcsUtil.getObjects(anyListOf(GcsPath.class)))
        .thenReturn(
            ImmutableList.of(
                GcsUtil.StorageObjectOrIOException.create(new FileNotFoundException("some/path"))));

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setFilesToStage(
        ImmutableList.of(
            temp1.getAbsolutePath(), overridePackageName + "=" + temp2.getAbsolutePath()));
    options.setStagingLocation(VALID_STAGING_BUCKET);
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setTempDatasetId(cloudDataflowDataset);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION_ID);
    options.setJobName("job");
    options.setDataflowClient(buildMockDataflow());
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    when(mockGcsUtil.create(any(GcsPath.class), anyString(), anyInt()))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    Pipeline p = buildDataflowPipeline(options);

    DataflowPipelineJob job = (DataflowPipelineJob) p.run();
    assertEquals("newid", job.getJobId());

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    Job workflowJob = jobCaptor.getValue();
    assertValidJob(workflowJob);

    assertEquals(2, workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().size());
    DataflowPackage workflowPackage1 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(0);
    assertThat(workflowPackage1.getName(), startsWith(temp1.getName()));
    DataflowPackage workflowPackage2 =
        workflowJob.getEnvironment().getWorkerPools().get(0).getPackages().get(1);
    assertEquals(overridePackageName, workflowPackage2.getName());

    assertEquals(
        GcsPath.fromUri(VALID_TEMP_BUCKET).toResourceName(),
        workflowJob.getEnvironment().getTempStoragePrefix());
    assertEquals(cloudDataflowDataset, workflowJob.getEnvironment().getDataset());
    assertEquals(
        DataflowRunnerInfo.getDataflowRunnerInfo().getName(),
        workflowJob.getEnvironment().getUserAgent().get("name"));
    assertEquals(
        DataflowRunnerInfo.getDataflowRunnerInfo().getVersion(),
        workflowJob.getEnvironment().getUserAgent().get("version"));
  }

  @Test
  public void runWithDefaultFilesToStage() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setFilesToStage(null);
    DataflowRunner.fromOptions(options);
    assertFalse(options.getFilesToStage().isEmpty());
  }

  @Test
  public void testGcsStagingLocationInitialization() throws Exception {
    // Set temp location (required), and check that staging location is set.
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setProject(PROJECT_ID);
    options.setGcpCredential(new TestCredential());
    options.setGcsUtil(mockGcsUtil);
    options.setRunner(DataflowRunner.class);

    DataflowRunner.fromOptions(options);

    assertNotNull(options.getStagingLocation());
  }

  @Test
  public void testInvalidGcpTempLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setGcpTempLocation("file://temp/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected a valid 'gs://' path but was given"));
    DataflowRunner.fromOptions(options);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonGcsTempLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setTempLocation("file://temp/location");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "DataflowRunner requires gcpTempLocation, "
            + "but failed to retrieve a value from PipelineOptions");
    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidStagingLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setStagingLocation("file://my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Expected a valid 'gs://' path but was given"));
    }
    options.setStagingLocation("my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Expected a valid 'gs://' path but was given"));
    }
  }

  @Test
  public void testInvalidProfileLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setSaveProfilesToGcs("file://my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Expected a valid 'gs://' path but was given"));
    }
    options.setSaveProfilesToGcs("my/staging/location");
    try {
      DataflowRunner.fromOptions(options);
      fail("fromOptions should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Expected a valid 'gs://' path but was given"));
    }
  }

  @Test
  public void testNonExistentTempLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setGcpTempLocation(NON_EXISTENT_BUCKET);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        containsString("Output path does not exist or is not writeable: " + NON_EXISTENT_BUCKET));
    DataflowRunner.fromOptions(options);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonExistentStagingLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setStagingLocation(NON_EXISTENT_BUCKET);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        containsString("Output path does not exist or is not writeable: " + NON_EXISTENT_BUCKET));
    DataflowRunner.fromOptions(options);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNonExistentProfileLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setSaveProfilesToGcs(NON_EXISTENT_BUCKET);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        containsString("Output path does not exist or is not writeable: " + NON_EXISTENT_BUCKET));
    DataflowRunner.fromOptions(options);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNoProjectFails() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    options.setRunner(DataflowRunner.class);
    // Explicitly set to null to prevent the default instance factory from reading credentials
    // from a user's environment, causing this test to fail.
    options.setProject(null);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project id");
    thrown.expectMessage("when running a Dataflow in the cloud");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectId() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-12345");

    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectPrefix() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("google.com:some-project-12345");

    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectNumber() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("12345");

    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project number");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectDescription() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("some project");

    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project description");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidNumberOfWorkerHarnessThreads() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    FileSystems.setDefaultPipelineOptions(options);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-12345");

    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);

    options.as(DataflowPipelineDebugOptions.class).setNumberOfWorkerHarnessThreads(-1);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Number of worker harness threads");
    thrown.expectMessage("Please make sure the value is non-negative.");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testNoStagingLocationAndNoTempLocationFails() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setProject("foo-project");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "DataflowRunner requires gcpTempLocation, "
            + "but failed to retrieve a value from PipelineOption");
    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testGcpTempAndNoTempLocationSucceeds() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setProject("foo-project");
    options.setGcpTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testTempLocationAndNoGcpTempLocationSucceeds() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setProject("foo-project");
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setGcsUtil(mockGcsUtil);

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testValidProfileLocation() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setSaveProfilesToGcs(VALID_PROFILE_BUCKET);

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidJobName() throws IOException {
    List<String> invalidNames = Arrays.asList("invalid_name", "0invalid", "invalid-");
    List<String> expectedReason =
        Arrays.asList("JobName invalid", "JobName invalid", "JobName invalid");

    for (int i = 0; i < invalidNames.size(); ++i) {
      DataflowPipelineOptions options = buildPipelineOptions();
      options.setJobName(invalidNames.get(i));

      try {
        DataflowRunner.fromOptions(options);
        fail("Expected IllegalArgumentException for jobName " + options.getJobName());
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(), containsString(expectedReason.get(i)));
      }
    }
  }

  @Test
  public void testValidJobName() throws IOException {
    List<String> names =
        Arrays.asList("ok", "Ok", "A-Ok", "ok-123", "this-one-is-fairly-long-01234567890123456789");

    for (String name : names) {
      DataflowPipelineOptions options = buildPipelineOptions();
      options.setJobName(name);

      DataflowRunner runner = DataflowRunner.fromOptions(options);
      assertNotNull(runner);
    }
  }

  @Test
  public void testGcsUploadBufferSizeIsUnsetForBatchWhenDefault() throws IOException {
    DataflowPipelineOptions batchOptions = buildPipelineOptions();
    batchOptions.setRunner(DataflowRunner.class);
    Pipeline.create(batchOptions);
    assertNull(batchOptions.getGcsUploadBufferSizeBytes());
  }

  @Test
  public void testGcsUploadBufferSizeIsSetForStreamingWhenDefault() throws IOException {
    DataflowPipelineOptions streamingOptions = buildPipelineOptions();
    streamingOptions.setStreaming(true);
    streamingOptions.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(streamingOptions);

    // Instantiation of a runner prior to run() currently has a side effect of mutating the options.
    // This could be tested by DataflowRunner.fromOptions(streamingOptions) but would not ensure
    // that the pipeline itself had the expected options set.
    p.run();

    assertEquals(
        DataflowRunner.GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT,
        streamingOptions.getGcsUploadBufferSizeBytes().intValue());
  }

  @Test
  public void testGcsUploadBufferSizeUnchangedWhenNotDefault() throws IOException {
    int gcsUploadBufferSizeBytes = 12345678;
    DataflowPipelineOptions batchOptions = buildPipelineOptions();
    batchOptions.setGcsUploadBufferSizeBytes(gcsUploadBufferSizeBytes);
    batchOptions.setRunner(DataflowRunner.class);
    Pipeline.create(batchOptions);
    assertEquals(gcsUploadBufferSizeBytes, batchOptions.getGcsUploadBufferSizeBytes().intValue());

    DataflowPipelineOptions streamingOptions = buildPipelineOptions();
    streamingOptions.setStreaming(true);
    streamingOptions.setGcsUploadBufferSizeBytes(gcsUploadBufferSizeBytes);
    streamingOptions.setRunner(DataflowRunner.class);
    Pipeline.create(streamingOptions);
    assertEquals(
        gcsUploadBufferSizeBytes, streamingOptions.getGcsUploadBufferSizeBytes().intValue());
  }

  /** A fake PTransform for testing. */
  public static class TestTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {
    public boolean translated = false;

    @Override
    public PCollection<Integer> expand(PCollection<Integer> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          input.isBounded(),
          input.getCoder());
    }
  }

  @Test
  public void testTransformTranslatorMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(Arrays.asList(1, 2, 3))).apply(new TestTransform());

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString("no translator registered"));
    DataflowPipelineTranslator.fromOptions(options)
        .translate(p, DataflowRunner.fromOptions(options), Collections.emptyList());

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testTransformTranslator() throws IOException {
    // Test that we can provide a custom translation
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);
    TestTransform transform = new TestTransform();

    p.apply(Create.of(Arrays.asList(1, 2, 3)).withCoder(BigEndianIntegerCoder.of()))
        .apply(transform);

    DataflowPipelineTranslator translator = DataflowRunner.fromOptions(options).getTranslator();

    DataflowPipelineTranslator.registerTransformTranslator(
        TestTransform.class,
        (transform1, context) -> {
          transform1.translated = true;

          // Note: This is about the minimum needed to fake out a
          // translation. This obviously isn't a real translation.
          TransformTranslator.StepTranslationContext stepContext =
              context.addStep(transform1, "TestTranslate");
          stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform1));
        });

    translator.translate(p, DataflowRunner.fromOptions(options), Collections.emptyList());
    assertTrue(transform.translated);
  }

  private void verifyMapStateUnsupported(PipelineOptions options) throws Exception {
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(KV.of(13, 42)))
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Integer>, Void>() {
                  @StateId("fizzle")
                  private final StateSpec<MapState<Void, Void>> voidState = StateSpecs.map();

                  @ProcessElement
                  public void process() {}
                }));

    thrown.expectMessage("MapState");
    thrown.expect(UnsupportedOperationException.class);
    p.run();
  }

  @Test
  public void testMapStateUnsupportedInBatch() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(false);
    verifyMapStateUnsupported(options);
  }

  @Test
  public void testMapStateUnsupportedInStreaming() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    verifyMapStateUnsupported(options);
  }

  private void verifySetStateUnsupported(PipelineOptions options) throws Exception {
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(KV.of(13, 42)))
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Integer>, Void>() {
                  @StateId("fizzle")
                  private final StateSpec<SetState<Void>> voidState = StateSpecs.set();

                  @ProcessElement
                  public void process() {}
                }));

    thrown.expectMessage("SetState");
    thrown.expect(UnsupportedOperationException.class);
    p.run();
  }

  @Test
  public void testSetStateUnsupportedInBatch() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(false);
    Pipeline.create(options);
    verifySetStateUnsupported(options);
  }

  @Test
  public void testSetStateUnsupportedInStreaming() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    verifySetStateUnsupported(options);
  }

  /** Records all the composite transforms visited within the Pipeline. */
  private static class CompositeTransformRecorder extends PipelineVisitor.Defaults {
    private List<PTransform<?, ?>> transforms = new ArrayList<>();

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      if (node.getTransform() != null) {
        transforms.add(node.getTransform());
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    public List<PTransform<?, ?>> getCompositeTransforms() {
      return transforms;
    }
  }

  @Test
  public void testApplyIsScopedToExactClass() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);

    Create.TimestampedValues<String> transform =
        Create.timestamped(Arrays.asList(TimestampedValue.of("TestString", Instant.now())));
    p.apply(transform);

    CompositeTransformRecorder recorder = new CompositeTransformRecorder();
    p.traverseTopologically(recorder);

    // The recorder will also have seen a Create.Values composite as well, but we can't obtain that
    // transform.
    assertThat(
        "Expected to have seen CreateTimestamped composite transform.",
        recorder.getCompositeTransforms(),
        hasItem(transform));
    assertThat(
        "Expected to have two composites, CreateTimestamped and Create.Values",
        recorder.getCompositeTransforms(),
        hasItem(Matchers.<PTransform<?, ?>>isA((Class) Create.Values.class)));
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setRunner(DataflowRunner.class);
    assertEquals("DataflowRunner#testjobname", DataflowRunner.fromOptions(options).toString());
  }

  /**
   * Tests that the {@link DataflowRunner} with {@code --templateLocation} returns normally when the
   * runner is successfully run.
   */
  @Test
  public void testTemplateRunnerFullCompletion() throws Exception {
    File existingFile = tmpFolder.newFile();
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setProject("test-project");
    options.setRunner(DataflowRunner.class);
    options.setTemplateLocation(existingFile.getPath());
    options.setTempLocation(tmpFolder.getRoot().getPath());
    Pipeline p = Pipeline.create(options);

    p.run();
    expectedLogs.verifyInfo("Template successfully created");
  }

  /**
   * Tests that the {@link DataflowRunner} with {@code --templateLocation} returns normally when the
   * runner is successfully run with upload_graph experiment turned on. The result template should
   * not contain raw steps and stepsLocation file should be set.
   */
  @Test
  public void testTemplateRunnerWithUploadGraph() throws Exception {
    File existingFile = tmpFolder.newFile();
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setExperiments(Arrays.asList("upload_graph"));
    options.setJobName("TestJobName");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setProject("test-project");
    options.setRunner(DataflowRunner.class);
    options.setTemplateLocation(existingFile.getPath());
    options.setTempLocation(tmpFolder.getRoot().getPath());
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(ImmutableList.of(1)));
    p.run();
    expectedLogs.verifyInfo("Template successfully created");
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = objectMapper.readTree(existingFile);
    assertEquals(0, node.get("steps").size());
    assertNotNull(node.get("stepsLocation"));
  }

  /**
   * Tests that the {@link DataflowRunner} with {@code --templateLocation} throws the appropriate
   * exception when an output file is not writable.
   */
  @Test
  public void testTemplateRunnerLoggedErrorForFile() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setRunner(DataflowRunner.class);
    options.setTemplateLocation("//bad/path");
    options.setProject("test-project");
    options.setTempLocation(tmpFolder.getRoot().getPath());
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    Pipeline p = Pipeline.create(options);

    thrown.expectMessage("Cannot create output file at");
    thrown.expect(RuntimeException.class);
    p.run();
  }

  @Test
  public void testWorkerHarnessContainerImage() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    // default image set
    options.setWorkerHarnessContainerImage("some-container");
    assertThat(getContainerImageForJob(options), equalTo("some-container"));

    // batch, legacy
    options.setWorkerHarnessContainerImage("gcr.io/IMAGE/foo");
    options.setExperiments(null);
    options.setStreaming(false);
    System.setProperty("java.specification.version", "1.8");
    assertThat(getContainerImageForJob(options), equalTo("gcr.io/beam-java-batch/foo"));
    // batch, legacy, jdk11
    options.setStreaming(false);
    System.setProperty("java.specification.version", "11");
    assertThat(getContainerImageForJob(options), equalTo("gcr.io/beam-java11-batch/foo"));
    // streaming, legacy
    System.setProperty("java.specification.version", "1.8");
    options.setStreaming(true);
    assertThat(getContainerImageForJob(options), equalTo("gcr.io/beam-java-streaming/foo"));
    // streaming, legacy, jdk11
    System.setProperty("java.specification.version", "11");
    assertThat(getContainerImageForJob(options), equalTo("gcr.io/beam-java11-streaming/foo"));
    // streaming, fnapi
    options.setExperiments(ImmutableList.of("experiment1", "beam_fn_api"));
    assertThat(getContainerImageForJob(options), equalTo("gcr.io/java/foo"));
  }

  @Test
  public void testStreamingWriteWithNoShardingReturnsNewTransform() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.as(DataflowPipelineWorkerPoolOptions.class).setMaxNumWorkers(10);
    testStreamingWriteOverride(options, 20);
  }

  @Test
  public void testStreamingWriteWithNoShardingReturnsNewTransformMaxWorkersUnset() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    testStreamingWriteOverride(options, StreamingShardedWriteFactory.DEFAULT_NUM_SHARDS);
  }

  private void verifyMergingStatefulParDoRejected(PipelineOptions options) throws Exception {
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(KV.of(13, 42)))
        .apply(Window.into(Sessions.withGapDuration(Duration.millis(1))))
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Integer>, Void>() {
                  @StateId("fizzle")
                  private final StateSpec<ValueState<Void>> voidState = StateSpecs.value();

                  @ProcessElement
                  public void process() {}
                }));

    thrown.expectMessage("merging");
    thrown.expect(UnsupportedOperationException.class);
    p.run();
  }

  @Test
  public void testMergingStatefulRejectedInStreaming() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    verifyMergingStatefulParDoRejected(options);
  }

  @Test
  public void testMergingStatefulRejectedInBatch() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(false);
    verifyMergingStatefulParDoRejected(options);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testBatchGroupIntoBatchesOverride() {
    // Ignore this test for streaming pipelines.
    assumeFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());

    final int batchSize = 2;
    List<KV<String, Integer>> testValues =
        Arrays.asList(KV.of("A", 1), KV.of("B", 0), KV.of("A", 2), KV.of("A", 4), KV.of("A", 8));
    PCollection<KV<String, Iterable<Integer>>> output =
        pipeline.apply(Create.of(testValues)).apply(GroupIntoBatches.ofSize(batchSize));
    PAssert.thatMultimap(output)
        .satisfies(
            new SerializableFunction<Map<String, Iterable<Iterable<Integer>>>, Void>() {
              @Override
              public Void apply(Map<String, Iterable<Iterable<Integer>>> input) {
                assertEquals(2, input.size());
                assertThat(input.keySet(), containsInAnyOrder("A", "B"));
                Map<String, Integer> sums = new HashMap<>();
                for (Map.Entry<String, Iterable<Iterable<Integer>>> entry : input.entrySet()) {
                  for (Iterable<Integer> batch : entry.getValue()) {
                    assertThat(Iterables.size(batch), lessThanOrEqualTo(batchSize));
                    for (Integer value : batch) {
                      sums.put(entry.getKey(), value + sums.getOrDefault(entry.getKey(), 0));
                    }
                  }
                }
                assertEquals(15, (int) sums.get("A"));
                assertEquals(0, (int) sums.get("B"));
                return null;
              }
            });
    pipeline.run();

    AtomicBoolean sawGroupIntoBatchesOverride = new AtomicBoolean(false);
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (node.getTransform() instanceof BatchGroupIntoBatches) {
              sawGroupIntoBatchesOverride.set(true);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    assertTrue(sawGroupIntoBatchesOverride.get());
  }

  private void testStreamingWriteOverride(PipelineOptions options, int expectedNumShards) {
    TestPipeline p = TestPipeline.fromOptions(options);

    StreamingShardedWriteFactory<Object, Void, Object> factory =
        new StreamingShardedWriteFactory<>(p.getOptions());
    WriteFiles<Object, Void, Object> original = WriteFiles.to(new TestSink(tmpFolder.toString()));
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));
    AppliedPTransform<PCollection<Object>, WriteFilesResult<Void>, WriteFiles<Object, Void, Object>>
        originalApplication =
            AppliedPTransform.of("writefiles", objs.expand(), Collections.emptyMap(), original, p);

    WriteFiles<Object, Void, Object> replacement =
        (WriteFiles<Object, Void, Object>)
            factory.getReplacementTransform(originalApplication).getTransform();
    assertThat(replacement, not(equalTo((Object) original)));
    assertThat(replacement.getNumShardsProvider().get(), equalTo(expectedNumShards));

    WriteFilesResult<Void> originalResult = objs.apply(original);
    WriteFilesResult<Void> replacementResult = objs.apply(replacement);
    Map<PValue, ReplacementOutput> res =
        factory.mapOutputs(originalResult.expand(), replacementResult);
    assertEquals(1, res.size());
    assertEquals(
        originalResult.getPerDestinationOutputFilenames(),
        res.get(replacementResult.getPerDestinationOutputFilenames()).getOriginal().getValue());
  }

  private static class TestSink extends FileBasedSink<Object, Void, Object> {
    @Override
    public void validate(PipelineOptions options) {}

    TestSink(String tmpFolder) {
      super(
          StaticValueProvider.of(FileSystems.matchNewResource(tmpFolder, true)),
          DynamicFileDestinations.constant(
              new FilenamePolicy() {
                @Override
                public ResourceId windowedFilename(
                    int shardNumber,
                    int numShards,
                    BoundedWindow window,
                    PaneInfo paneInfo,
                    OutputFileHints outputFileHints) {
                  throw new UnsupportedOperationException("should not be called");
                }

                @Nullable
                @Override
                public ResourceId unwindowedFilename(
                    int shardNumber, int numShards, OutputFileHints outputFileHints) {
                  throw new UnsupportedOperationException("should not be called");
                }
              },
              SerializableFunctions.identity()));
    }

    @Override
    public WriteOperation<Void, Object> createWriteOperation() {
      return new WriteOperation<Void, Object>(this) {
        @Override
        public Writer<Void, Object> createWriter() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
