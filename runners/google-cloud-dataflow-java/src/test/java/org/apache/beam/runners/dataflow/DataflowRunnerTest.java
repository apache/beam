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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files.getFileExtension;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

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
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.SdkHarnessContainerImage;
import com.google.api.services.storage.model.StorageObject;
import com.google.auto.service.AutoService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ExpansionServiceClient;
import org.apache.beam.runners.core.construction.ExpansionServiceClientFactory;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.dataflow.DataflowRunner.StreamingShardedWriteFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DefaultGcpRegionFactory;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ExperimentalOptions;
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
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for the {@link DataflowRunner}.
 *
 * <p>Implements {@link Serializable} because it is caught in closures.
 */
@RunWith(JUnit4.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
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
    mockGcsUtil = buildMockGcsUtil();
    mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);
  }

  private static Pipeline buildDataflowPipeline(DataflowPipelineOptions options) {
    options.setStableUniqueNames(CheckEnabled.ERROR);
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadMyFile", TextIO.read().from("gs://bucket/object"))
        .apply("WriteMyFile", TextIO.write().to("gs://bucket/object"));

    // Enable the FileSystems API to know about gs:// URIs in this test.
    FileSystems.setDefaultPipelineOptions(options);

    return p;
  }

  private static Pipeline buildDataflowPipelineWithLargeGraph(DataflowPipelineOptions options) {
    options.setStableUniqueNames(CheckEnabled.ERROR);
    options.setRunner(DataflowRunner.class);
    Pipeline p = Pipeline.create(options);

    for (int i = 0; i < 100; i++) {
      p.apply("ReadMyFile_" + i, TextIO.read().from("gs://bucket/object"))
          .apply("WriteMyFile_" + i, TextIO.write().to("gs://bucket/object"));
    }

    // Enable the FileSystems API to know about gs:// URIs in this test.
    FileSystems.setDefaultPipelineOptions(options);

    return p;
  }

  private static Dataflow buildMockDataflow(Dataflow.Projects.Locations.Jobs mockJobs)
      throws IOException {
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

  private static GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);

    when(mockGcsUtil.create(any(GcsPath.class), any(GcsUtil.CreateOptions.class)))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    when(mockGcsUtil.create(any(GcsPath.class), any(GcsUtil.CreateOptions.class)))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    when(mockGcsUtil.expand(any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));
    doNothing().when(mockGcsUtil).verifyBucketAccessible(GcsPath.fromUri(VALID_STAGING_BUCKET));
    doNothing().when(mockGcsUtil).verifyBucketAccessible(GcsPath.fromUri(VALID_TEMP_BUCKET));
    doNothing()
        .when(mockGcsUtil)
        .verifyBucketAccessible(GcsPath.fromUri(VALID_TEMP_BUCKET + "/staging"));
    doNothing().when(mockGcsUtil).verifyBucketAccessible(GcsPath.fromUri(VALID_PROFILE_BUCKET));
    doThrow(new FileNotFoundException())
        .when(mockGcsUtil)
        .verifyBucketAccessible(GcsPath.fromUri(NON_EXISTENT_BUCKET));

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
    doNothing().when(mockGcsUtil).verifyBucketAccessible(GcsPath.fromUri("gs://bucket/object"));

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
    options.setDataflowClient(buildMockDataflow(mockJobs));
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
          "--region=some-region-1",
          "--tempLocation=/tmp/not/a/gs/path",
          "--project=test-project",
          "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
        };

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("DataflowRunner requires gcpTempLocation");
    Pipeline.create(PipelineOptionsFactory.fromArgs(args).create()).run();
  }

  @Test
  public void testPathExistsValidation() {
    String[] args =
        new String[] {
          "--runner=DataflowRunner",
          "--region=some-region-1",
          "--tempLocation=gs://does/not/exist",
          "--project=test-project",
          "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
        };

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("gcpTempLocation");
    thrown.expectCause(hasProperty("message", containsString("gs://does/not/exist")));
    Pipeline.create(PipelineOptionsFactory.fromArgs(args).create()).run();
  }

  @Test
  public void testPathValidatorOverride() {
    String[] args =
        new String[] {
          "--runner=DataflowRunner",
          "--region=some-region-1",
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
    assertThat(optionsMap, hasEntry("region", (Object) options.getRegion()));
  }

  /**
   * Test that the region is set in the generated JSON pipeline options even when a default value is
   * grabbed from the environment.
   */
  @RunWith(PowerMockRunner.class)
  @PrepareForTest(DefaultGcpRegionFactory.class)
  public static class DefaultRegionTest {
    @Test
    public void testDefaultRegionSet() throws Exception {
      mockStatic(DefaultGcpRegionFactory.class);
      PowerMockito.when(DefaultGcpRegionFactory.getRegionFromEnvironment()).thenReturn(REGION_ID);
      Dataflow.Projects.Locations.Jobs mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);

      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      options.setRunner(DataflowRunner.class);
      options.setProject(PROJECT_ID);
      options.setTempLocation(VALID_TEMP_BUCKET);
      // Set FILES_PROPERTY to empty to prevent a default value calculated from classpath.
      options.setFilesToStage(new ArrayList<>());
      options.setDataflowClient(buildMockDataflow(mockJobs));
      options.setGcsUtil(buildMockGcsUtil());
      options.setGcpCredential(new TestCredential());

      Pipeline p = Pipeline.create(options);
      p.run();

      ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
      Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
      Map<String, Object> sdkPipelineOptions =
          jobCaptor.getValue().getEnvironment().getSdkPipelineOptions();

      assertThat(sdkPipelineOptions, hasKey("options"));
      Map<String, Object> optionsMap = (Map<String, Object>) sdkPipelineOptions.get("options");
      assertThat(optionsMap, hasEntry("region", (Object) options.getRegion()));
    }
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
  public void testZoneAndWorkerRegionMutuallyExclusive() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    options.setZone("us-east1-b");
    options.setWorkerRegion("us-east1");
    assertThrows(
        IllegalArgumentException.class, () -> DataflowRunner.validateWorkerSettings(options));
  }

  @Test
  public void testZoneAndWorkerZoneMutuallyExclusive() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    options.setZone("us-east1-b");
    options.setWorkerZone("us-east1-c");
    assertThrows(
        IllegalArgumentException.class, () -> DataflowRunner.validateWorkerSettings(options));
  }

  @Test
  public void testExperimentRegionAndWorkerRegionMutuallyExclusive() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    ExperimentalOptions.addExperiment(dataflowOptions, "worker_region=us-west1");
    options.setWorkerRegion("us-east1");
    assertThrows(
        IllegalArgumentException.class, () -> DataflowRunner.validateWorkerSettings(options));
  }

  @Test
  public void testExperimentRegionAndWorkerZoneMutuallyExclusive() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    ExperimentalOptions.addExperiment(dataflowOptions, "worker_region=us-west1");
    options.setWorkerZone("us-east1-b");
    assertThrows(
        IllegalArgumentException.class, () -> DataflowRunner.validateWorkerSettings(options));
  }

  @Test
  public void testWorkerRegionAndWorkerZoneMutuallyExclusive() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    options.setWorkerRegion("us-east1");
    options.setWorkerZone("us-east1-b");
    assertThrows(
        IllegalArgumentException.class, () -> DataflowRunner.validateWorkerSettings(options));
  }

  @Test
  public void testZoneAliasWorkerZone() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    options.setZone("us-east1-b");
    DataflowRunner.validateWorkerSettings(options);
    assertNull(options.getZone());
    assertEquals("us-east1-b", options.getWorkerZone());
  }

  @Test
  public void testAliasForLegacyWorkerHarnessContainerImage() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    String testImage = "image.url:worker";
    options.setWorkerHarnessContainerImage(testImage);
    DataflowRunner.validateWorkerSettings(options);
    assertEquals(testImage, options.getWorkerHarnessContainerImage());
    assertEquals(testImage, options.getSdkContainerImage());
  }

  @Test
  public void testAliasForSdkContainerImage() {
    DataflowPipelineWorkerPoolOptions options =
        PipelineOptionsFactory.as(DataflowPipelineWorkerPoolOptions.class);
    String testImage = "image.url:sdk";
    options.setSdkContainerImage("image.url:sdk");
    DataflowRunner.validateWorkerSettings(options);
    assertEquals(testImage, options.getWorkerHarnessContainerImage());
    assertEquals(testImage, options.getSdkContainerImage());
  }

  @Test
  public void testRegionRequiredForServiceRunner() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRegion(null);
    options.setDataflowEndpoint("https://dataflow.googleapis.com");
    assertThrows(IllegalArgumentException.class, () -> DataflowRunner.fromOptions(options));
  }

  @Test
  public void testRegionOptionalForNonServiceRunner() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRegion(null);
    options.setDataflowEndpoint("http://localhost:20281");
    DataflowRunner.fromOptions(options);
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
    DataflowRunner.fromOptions(dataflowOptions).replaceV1Transforms(p);
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
    p.run();

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

  /** Test for automatically using upload_graph when the job graph is too large (>10MB). */
  @Test
  public void testUploadGraphWithAutoUpload() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = buildDataflowPipelineWithLargeGraph(options);
    p.run();

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
    File temp1 = File.createTempFile("DataflowRunnerTest-", ".txt");
    temp1.deleteOnExit();
    File temp2 = File.createTempFile("DataflowRunnerTest2-", ".txt");
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
    options.setDataflowClient(buildMockDataflow(mockJobs));
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    when(mockGcsUtil.create(any(GcsPath.class), any(GcsUtil.CreateOptions.class)))
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
    assertThat(workflowPackage1.getName(), endsWith(getFileExtension(temp1.getAbsolutePath())));
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

  /**
   * Tests that {@link DataflowRunner} throws an appropriate exception when an explicitly specified
   * file to stage does not exist locally.
   */
  @Test(expected = RuntimeException.class)
  public void testRunWithMissingFiles() throws IOException {
    final String cloudDataflowDataset = "somedataset";

    File temp = new File("/this/is/not/a/path/that/will/exist");

    String overridePackageName = "alias.txt";

    when(mockGcsUtil.getObjects(anyListOf(GcsPath.class)))
        .thenReturn(
            ImmutableList.of(
                GcsUtil.StorageObjectOrIOException.create(new FileNotFoundException("some/path"))));

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setFilesToStage(ImmutableList.of(overridePackageName + "=" + temp.getAbsolutePath()));
    options.setStagingLocation(VALID_STAGING_BUCKET);
    options.setTempLocation(VALID_TEMP_BUCKET);
    options.setTempDatasetId(cloudDataflowDataset);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION_ID);
    options.setJobName("job");
    options.setDataflowClient(buildMockDataflow(mockJobs));
    options.setGcsUtil(mockGcsUtil);
    options.setGcpCredential(new TestCredential());

    when(mockGcsUtil.create(any(GcsPath.class), any(GcsUtil.CreateOptions.class)))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    Pipeline p = buildDataflowPipeline(options);
    p.run();
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
    options.setRegion(REGION_ID);
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

    thrown.expect(RuntimeException.class);
    thrown.expectCause(instanceOf(FileNotFoundException.class));
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

    thrown.expect(RuntimeException.class);
    thrown.expectCause(instanceOf(FileNotFoundException.class));
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

    thrown.expect(RuntimeException.class);
    thrown.expectCause(instanceOf(FileNotFoundException.class));
    thrown.expectMessage(
        containsString("Output path does not exist or is not writeable: " + NON_EXISTENT_BUCKET));
    DataflowRunner.fromOptions(options);

    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(mockJobs).create(eq(PROJECT_ID), eq(REGION_ID), jobCaptor.capture());
    assertValidJob(jobCaptor.getValue());
  }

  @Test
  public void testNoProjectFails() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();

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
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setProject("foo-12345");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectPrefix() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setProject("google.com:some-project-12345");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectNumber() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setProject("12345");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project number");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testProjectDescription() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setProject("some project");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Project ID");
    thrown.expectMessage("project description");

    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testInvalidNumberOfWorkerHarnessThreads() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
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
    options.setRegion(REGION_ID);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "DataflowRunner requires gcpTempLocation, "
            + "but failed to retrieve a value from PipelineOption");
    DataflowRunner.fromOptions(options);
  }

  @Test
  public void testApplySdkEnvironmentOverrides() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    String dockerHubPythonContainerUrl = "apache/beam_python3.8_sdk:latest";
    String gcrPythonContainerUrl = "gcr.io/apache-beam-testing/beam-sdk/beam_python3.8_sdk:latest";
    options.setSdkHarnessContainerImageOverrides(".*python.*," + gcrPythonContainerUrl);
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .setUrn(
                                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER))
                            .setPayload(
                                RunnerApi.DockerPayload.newBuilder()
                                    .setContainerImage(dockerHubPythonContainerUrl)
                                    .build()
                                    .toByteString())
                            .build()))
            .build();
    RunnerApi.Pipeline expectedPipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .setUrn(
                                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER))
                            .setPayload(
                                RunnerApi.DockerPayload.newBuilder()
                                    .setContainerImage(gcrPythonContainerUrl)
                                    .build()
                                    .toByteString())
                            .build()))
            .build();
    assertThat(runner.applySdkEnvironmentOverrides(pipeline, options), equalTo(expectedPipeline));
  }

  @Test
  public void testApplySdkEnvironmentOverridesByDefault() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    String dockerHubPythonContainerUrl = "apache/beam_python3.8_sdk:latest";
    String gcrPythonContainerUrl = "gcr.io/cloud-dataflow/v1beta3/beam_python3.8_sdk:latest";
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .setUrn(
                                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER))
                            .setPayload(
                                RunnerApi.DockerPayload.newBuilder()
                                    .setContainerImage(dockerHubPythonContainerUrl)
                                    .build()
                                    .toByteString())
                            .build()))
            .build();
    RunnerApi.Pipeline expectedPipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .setUrn(
                                BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER))
                            .setPayload(
                                RunnerApi.DockerPayload.newBuilder()
                                    .setContainerImage(gcrPythonContainerUrl)
                                    .build()
                                    .toByteString())
                            .build()))
            .build();
    assertThat(runner.applySdkEnvironmentOverrides(pipeline, options), equalTo(expectedPipeline));
  }

  @Test
  public void testStageArtifactWithoutStagedName() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);

    File temp1 = File.createTempFile("artifact1-", ".txt");
    temp1.deleteOnExit();
    File temp2 = File.createTempFile("artifact2-", ".txt");
    temp2.deleteOnExit();

    RunnerApi.ArtifactInformation fooLocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(temp1.getAbsolutePath())
                    .build()
                    .toByteString())
            .build();
    RunnerApi.ArtifactInformation barLocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(temp2.getAbsolutePath())
                    .build()
                    .toByteString())
            .build();
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .addAllDependencies(
                                ImmutableList.of(fooLocalArtifact, barLocalArtifact))
                            .build()))
            .build();
    List<DataflowPackage> packages = runner.stageArtifacts(pipeline);
    for (DataflowPackage pkg : packages) {
      assertThat(pkg.getName(), matchesRegex("artifact[1,2]-.+\\.txt"));
    }
  }

  @Test
  public void testStageDuplicatedArtifacts() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);

    File foo = File.createTempFile("foo-", ".txt");
    foo.deleteOnExit();
    File bar = File.createTempFile("bar-", ".txt");
    bar.deleteOnExit();

    RunnerApi.ArtifactInformation foo1LocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(foo.getAbsolutePath())
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("foo_staged1.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.ArtifactInformation foo2LocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(foo.getAbsolutePath())
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("foo_staged2.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.ArtifactInformation barLocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(bar.getAbsolutePath())
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("bar_staged.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.Environment env1 =
        RunnerApi.Environment.newBuilder()
            .addAllDependencies(ImmutableList.of(foo1LocalArtifact, barLocalArtifact))
            .build();
    RunnerApi.Environment env2 =
        RunnerApi.Environment.newBuilder()
            .addAllDependencies(ImmutableList.of(foo2LocalArtifact, barLocalArtifact))
            .build();
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments("env1", env1)
                    .putEnvironments("env2", env2))
            .build();
    List<DataflowPackage> packages = runner.stageArtifacts(pipeline);
    List<String> packageNames =
        packages.stream().map(DataflowPackage::getName).collect(Collectors.toList());
    assertThat(packageNames.size(), equalTo(3));
    assertThat(
        packageNames, containsInAnyOrder("foo_staged1.jar", "foo_staged2.jar", "bar_staged.jar"));
  }

  @Test
  public void testResolveArtifacts() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    DataflowRunner runner = DataflowRunner.fromOptions(options);
    String stagingLocation = options.getStagingLocation().replaceFirst("/$", "");
    RunnerApi.ArtifactInformation fooLocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath("/tmp/foo.jar")
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("foo_staged.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.ArtifactInformation barLocalArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath("/tmp/bar.jar")
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("bar_staged.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .addAllDependencies(
                                ImmutableList.of(fooLocalArtifact, barLocalArtifact))
                            .build()))
            .build();

    RunnerApi.ArtifactInformation fooStagedArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.URL))
            .setTypePayload(
                RunnerApi.ArtifactUrlPayload.newBuilder()
                    .setUrl(stagingLocation + "/foo_staged.jar")
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("foo_staged.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.ArtifactInformation barStagedArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.URL))
            .setTypePayload(
                RunnerApi.ArtifactUrlPayload.newBuilder()
                    .setUrl(stagingLocation + "/bar_staged.jar")
                    .build()
                    .toByteString())
            .setRoleUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO))
            .setRolePayload(
                RunnerApi.ArtifactStagingToRolePayload.newBuilder()
                    .setStagedName("bar_staged.jar")
                    .build()
                    .toByteString())
            .build();
    RunnerApi.Pipeline expectedPipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env",
                        RunnerApi.Environment.newBuilder()
                            .addAllDependencies(
                                ImmutableList.of(fooStagedArtifact, barStagedArtifact))
                            .build()))
            .build();
    assertThat(runner.resolveArtifacts(pipeline), equalTo(expectedPipeline));
  }

  @Test
  public void testGcpTempAndNoTempLocationSucceeds() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setProject("foo-project");
    options.setRegion(REGION_ID);
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
    options.setRegion(REGION_ID);
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

  private static class TestTransformTranslator
      implements TransformPayloadTranslator<TestTransform> {
    @Override
    public String getUrn() {
      return "test_transform";
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, TestTransform> application, SdkComponents components)
        throws IOException {
      return RunnerApi.FunctionSpec.newBuilder().setUrn(getUrn(application.getTransform())).build();
    }
  }

  @SuppressWarnings({
    "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
  })
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class DataflowTransformTranslator implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.of(TestTransform.class, new TestTransformTranslator());
    }
  }

  @Test
  public void testTransformTranslatorMissing() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(Arrays.asList(1, 2, 3))).apply(new TestTransform());

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString("no translator registered"));
    SdkComponents sdkComponents = SdkComponents.create(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    DataflowPipelineTranslator.fromOptions(options)
        .translate(
            p,
            pipelineProto,
            sdkComponents,
            DataflowRunner.fromOptions(options),
            Collections.emptyList());

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

    SdkComponents sdkComponents = SdkComponents.create(options);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);
    translator.translate(
        p,
        pipelineProto,
        sdkComponents,
        DataflowRunner.fromOptions(options),
        Collections.emptyList());
    assertTrue(transform.translated);
  }

  private void verifySdkHarnessConfiguration(DataflowPipelineOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(Arrays.asList(1, 2, 3)));

    String defaultSdkContainerImage = DataflowRunner.getContainerImageForJob(options);
    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.Environment defaultEnvironmentForDataflow =
        Environments.createDockerEnvironment(defaultSdkContainerImage);
    RunnerApi.Environment.Builder envBuilder =
        defaultEnvironmentForDataflow.toBuilder().addCapabilities("my_dummy_capability");
    sdkComponents.registerEnvironment(envBuilder.build());

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

    DataflowRunner.configureSdkHarnessContainerImages(options, pipelineProto, job);
    List<SdkHarnessContainerImage> sdks =
        job.getEnvironment().getWorkerPools().get(0).getSdkHarnessContainerImages();

    Map<String, String> expectedEnvIdsAndContainerImages =
        pipelineProto.getComponents().getEnvironmentsMap().entrySet().stream()
            .filter(
                x ->
                    BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER)
                        .equals(x.getValue().getUrn()))
            .collect(
                Collectors.toMap(
                    x -> x.getKey(),
                    x -> {
                      RunnerApi.DockerPayload payload;
                      try {
                        payload = RunnerApi.DockerPayload.parseFrom(x.getValue().getPayload());
                      } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                      }
                      return payload.getContainerImage();
                    }));

    assertEquals(1, expectedEnvIdsAndContainerImages.size());
    assertEquals(1, sdks.size());
    assertEquals(
        expectedEnvIdsAndContainerImages,
        sdks.stream()
            .collect(
                Collectors.toMap(
                    SdkHarnessContainerImage::getEnvironmentId,
                    SdkHarnessContainerImage::getContainerImage)));
    assertTrue(sdks.get(0).getCapabilities().contains("my_dummy_capability"));
  }

  @Test
  public void testSdkHarnessConfigurationRunnerV2() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    ExperimentalOptions.addExperiment(options, "use_runner_v2");
    this.verifySdkHarnessConfiguration(options);
  }

  @Test
  public void testSdkHarnessConfigurationPrime() throws IOException {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setDataflowServiceOptions(ImmutableList.of("enable_prime"));
    this.verifySdkHarnessConfiguration(options);
  }

  @Test
  public void testSettingAnyFnApiExperimentEnablesUnifiedWorker() throws Exception {
    for (String experiment :
        ImmutableList.of(
            "beam_fn_api", "use_runner_v2", "use_unified_worker", "use_portable_job_submission")) {
      DataflowPipelineOptions options = buildPipelineOptions();
      ExperimentalOptions.addExperiment(options, experiment);
      Pipeline p = Pipeline.create(options);
      p.apply(Create.of("A"));
      p.run();
      assertFalse(options.isEnableStreamingEngine());
      assertThat(
          options.getExperiments(),
          containsInAnyOrder(
              "beam_fn_api", "use_runner_v2", "use_unified_worker", "use_portable_job_submission"));
    }

    for (String experiment :
        ImmutableList.of(
            "beam_fn_api", "use_runner_v2", "use_unified_worker", "use_portable_job_submission")) {
      DataflowPipelineOptions options = buildPipelineOptions();
      options.setStreaming(true);
      ExperimentalOptions.addExperiment(options, experiment);
      Pipeline p = Pipeline.create(options);
      p.apply(Create.of("A"));
      p.run();
      assertTrue(options.isEnableStreamingEngine());
      assertThat(
          options.getExperiments(),
          containsInAnyOrder(
              "beam_fn_api",
              "use_runner_v2",
              "use_unified_worker",
              "use_portable_job_submission",
              "enable_windmill_service",
              "enable_streaming_engine"));
    }
  }

  @Test
  public void testSettingConflictingEnableAndDisableExperimentsThrowsException() throws Exception {
    for (String experiment :
        ImmutableList.of(
            "beam_fn_api", "use_runner_v2", "use_unified_worker", "use_portable_job_submission")) {
      for (String disabledExperiment :
          ImmutableList.of(
              "disable_runner_v2", "disable_runner_v2_until_2023", "disable_prime_runner_v2")) {
        DataflowPipelineOptions options = buildPipelineOptions();
        ExperimentalOptions.addExperiment(options, experiment);
        ExperimentalOptions.addExperiment(options, disabledExperiment);
        Pipeline p = Pipeline.create(options);
        p.apply(Create.of("A"));
        assertThrows("Runner V2 both disabled and enabled", IllegalArgumentException.class, p::run);
      }
    }
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
  public void testMapStateUnsupportedStreamingEngine() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    ExperimentalOptions.addExperiment(
        options.as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    options.as(DataflowPipelineOptions.class).setStreaming(true);

    verifyMapStateUnsupported(options);
  }

  @Test
  public void testMapStateUnsupportedStreamingUnifiedRunner() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), "use_unified_worker");
    options.as(DataflowPipelineOptions.class).setStreaming(true);

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
  public void testSetStateUnsupportedStreamingEngine() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    ExperimentalOptions.addExperiment(
        options.as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    options.as(DataflowPipelineOptions.class).setStreaming(true);
    verifySetStateUnsupported(options);
  }

  @Test
  public void testSetStateUnsupportedStreamingUnifiedWorker() throws Exception {
    PipelineOptions options = buildPipelineOptions();
    ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), "use_unified_worker");
    options.as(DataflowPipelineOptions.class).setStreaming(true);
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
    options.setRegion(REGION_ID);
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
    options.setRegion(REGION_ID);
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
    options.setRegion(REGION_ID);
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
    options.setRegion(REGION_ID);
    options.setTempLocation(tmpFolder.getRoot().getPath());
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    Pipeline p = Pipeline.create(options);

    thrown.expectMessage("Cannot create output file at");
    thrown.expect(RuntimeException.class);
    p.run();
  }

  @Test
  public void testGetContainerImageForJobFromOption() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    String[] testCases = {
      "some-container",

      // It is important that empty string is preserved, as
      // dataflowWorkerJar relies on being passed an empty value vs
      // not providing the container image option at all.
      "",
    };

    for (String testCase : testCases) {
      // When image option is set, should use that exact image.
      options.setSdkContainerImage(testCase);
      assertThat(getContainerImageForJob(options), equalTo(testCase));
    }
  }

  @Test
  public void testGetContainerImageForJobFromOptionWithPlaceholder() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setSdkContainerImage("gcr.io/IMAGE/foo");

    for (Environments.JavaVersion javaVersion : Environments.JavaVersion.values()) {
      System.setProperty("java.specification.version", javaVersion.specification());
      // batch legacy
      options.setExperiments(null);
      options.setStreaming(false);
      assertThat(
          getContainerImageForJob(options),
          equalTo(String.format("gcr.io/beam-%s-batch/foo", javaVersion.legacyName())));

      // streaming, legacy
      options.setExperiments(null);
      options.setStreaming(true);
      assertThat(
          getContainerImageForJob(options),
          equalTo(String.format("gcr.io/beam-%s-streaming/foo", javaVersion.legacyName())));

      // batch, FnAPI
      options.setExperiments(ImmutableList.of("beam_fn_api"));
      options.setStreaming(false);
      assertThat(
          getContainerImageForJob(options),
          equalTo(String.format("gcr.io/beam_%s_sdk/foo", javaVersion.name())));

      // streaming, FnAPI
      options.setExperiments(ImmutableList.of("beam_fn_api"));
      options.setStreaming(true);
      assertThat(
          getContainerImageForJob(options),
          equalTo(String.format("gcr.io/beam_%s_sdk/foo", javaVersion.name())));
    }
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

  private void verifyGroupIntoBatchesOverrideCount(
      Pipeline p, Boolean withShardedKey, Boolean expectOverriden) {
    final int batchSize = 2;
    List<KV<String, Integer>> testValues =
        Arrays.asList(KV.of("A", 1), KV.of("B", 0), KV.of("A", 2), KV.of("A", 4), KV.of("A", 8));
    PCollection<KV<String, Integer>> input = p.apply("CreateValuesCount", Create.of(testValues));
    PCollection<KV<String, Iterable<Integer>>> output;
    if (withShardedKey) {
      output =
          input
              .apply(
                  "GroupIntoBatchesCount",
                  GroupIntoBatches.<String, Integer>ofSize(batchSize).withShardedKey())
              .apply(
                  "StripShardIdCount",
                  MapElements.via(
                      new SimpleFunction<
                          KV<ShardedKey<String>, Iterable<Integer>>,
                          KV<String, Iterable<Integer>>>() {
                        @Override
                        public KV<String, Iterable<Integer>> apply(
                            KV<ShardedKey<String>, Iterable<Integer>> input) {
                          return KV.of(input.getKey().getKey(), input.getValue());
                        }
                      }));
    } else {
      output = input.apply("GroupIntoBatchesCount", GroupIntoBatches.ofSize(batchSize));
    }
    PAssert.thatMultimap(output)
        .satisfies(
            i -> {
              assertEquals(2, i.size());
              assertThat(i.keySet(), containsInAnyOrder("A", "B"));
              Map<String, Integer> sums = new HashMap<>();
              for (Map.Entry<String, Iterable<Iterable<Integer>>> entry : i.entrySet()) {
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
            });
    p.run();

    AtomicBoolean sawGroupIntoBatchesOverride = new AtomicBoolean(false);
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform()
                    instanceof GroupIntoBatchesOverride.StreamingGroupIntoBatchesWithShardedKey) {
              sawGroupIntoBatchesOverride.set(true);
            }
            if (!p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform() instanceof GroupIntoBatchesOverride.BatchGroupIntoBatches) {
              sawGroupIntoBatchesOverride.set(true);
            }
            if (!p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform()
                    instanceof GroupIntoBatchesOverride.BatchGroupIntoBatchesWithShardedKey) {
              sawGroupIntoBatchesOverride.set(true);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    if (expectOverriden) {
      assertTrue(sawGroupIntoBatchesOverride.get());
    } else {
      assertFalse(sawGroupIntoBatchesOverride.get());
    }
  }

  private void verifyGroupIntoBatchesOverrideBytes(
      Pipeline p, Boolean withShardedKey, Boolean expectOverriden) {
    final long batchSizeBytes = 2;
    List<KV<String, String>> testValues =
        Arrays.asList(
            KV.of("A", "a"),
            KV.of("A", "ab"),
            KV.of("A", "abc"),
            KV.of("A", "abcd"),
            KV.of("A", "abcde"));
    PCollection<KV<String, String>> input = p.apply("CreateValuesBytes", Create.of(testValues));
    PCollection<KV<String, Iterable<String>>> output;
    if (withShardedKey) {
      output =
          input
              .apply(
                  "GroupIntoBatchesBytes",
                  GroupIntoBatches.<String, String>ofByteSize(batchSizeBytes).withShardedKey())
              .apply(
                  "StripShardIdBytes",
                  MapElements.via(
                      new SimpleFunction<
                          KV<ShardedKey<String>, Iterable<String>>,
                          KV<String, Iterable<String>>>() {
                        @Override
                        public KV<String, Iterable<String>> apply(
                            KV<ShardedKey<String>, Iterable<String>> input) {
                          return KV.of(input.getKey().getKey(), input.getValue());
                        }
                      }));
    } else {
      output = input.apply("GroupIntoBatchesBytes", GroupIntoBatches.ofByteSize(batchSizeBytes));
    }
    PAssert.thatMultimap(output)
        .satisfies(
            i -> {
              assertEquals(1, i.size());
              assertThat(i.keySet(), containsInAnyOrder("A"));
              Iterable<Iterable<String>> batches = i.get("A");
              assertEquals(5, Iterables.size(batches));
              return null;
            });
    p.run();

    AtomicBoolean sawGroupIntoBatchesOverride = new AtomicBoolean(false);
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform()
                    instanceof GroupIntoBatchesOverride.StreamingGroupIntoBatchesWithShardedKey) {
              sawGroupIntoBatchesOverride.set(true);
            }
            if (!p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform() instanceof GroupIntoBatchesOverride.BatchGroupIntoBatches) {
              sawGroupIntoBatchesOverride.set(true);
            }
            if (!p.getOptions().as(StreamingOptions.class).isStreaming()
                && node.getTransform()
                    instanceof GroupIntoBatchesOverride.BatchGroupIntoBatchesWithShardedKey) {
              sawGroupIntoBatchesOverride.set(true);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    if (expectOverriden) {
      assertTrue(sawGroupIntoBatchesOverride.get());
    } else {
      assertFalse(sawGroupIntoBatchesOverride.get());
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class})
  public void testBatchGroupIntoBatchesOverrideCount() {
    // Ignore this test for streaming pipelines.
    assumeFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());
    verifyGroupIntoBatchesOverrideCount(pipeline, false, true);
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class})
  public void testBatchGroupIntoBatchesOverrideBytes() {
    // Ignore this test for streaming pipelines.
    assumeFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());
    verifyGroupIntoBatchesOverrideBytes(pipeline, false, true);
  }

  @Test
  public void testBatchGroupIntoBatchesWithShardedKeyOverrideCount() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideCount(p, true, true);
  }

  @Test
  public void testBatchGroupIntoBatchesWithShardedKeyOverrideBytes() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideBytes(p, true, true);
  }

  @Test
  public void testStreamingGroupIntoBatchesOverrideCount() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideCount(p, false, false);
  }

  @Test
  public void testStreamingGroupIntoBatchesOverrideBytes() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    options.as(StreamingOptions.class).setStreaming(true);
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideBytes(p, false, false);
  }

  @Test
  public void testStreamingGroupIntoBatchesWithShardedKeyOverrideCount() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    List<String> experiments =
        new ArrayList<>(
            ImmutableList.of(
                GcpOptions.STREAMING_ENGINE_EXPERIMENT,
                GcpOptions.WINDMILL_SERVICE_EXPERIMENT,
                "use_runner_v2"));
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setExperiments(experiments);
    dataflowOptions.setStreaming(true);
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideCount(p, true, true);
  }

  @Test
  public void testStreamingGroupIntoBatchesWithShardedKeyOverrideBytes() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    List<String> experiments =
        new ArrayList<>(
            ImmutableList.of(
                GcpOptions.STREAMING_ENGINE_EXPERIMENT,
                GcpOptions.WINDMILL_SERVICE_EXPERIMENT,
                "use_runner_v2"));
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setExperiments(experiments);
    dataflowOptions.setStreaming(true);
    Pipeline p = Pipeline.create(options);
    verifyGroupIntoBatchesOverrideBytes(p, true, true);
  }

  @Test
  public void testPubsubSinkOverride() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    List<String> experiments =
        new ArrayList<>(
            ImmutableList.of(
                GcpOptions.STREAMING_ENGINE_EXPERIMENT,
                GcpOptions.WINDMILL_SERVICE_EXPERIMENT,
                "use_runner_v2"));
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setExperiments(experiments);
    dataflowOptions.setStreaming(true);
    Pipeline p = Pipeline.create(options);

    List<PubsubMessage> testValues =
        Arrays.asList(
            new PubsubMessage("foo".getBytes(StandardCharsets.UTF_8), Collections.emptyMap()));
    PCollection<PubsubMessage> input =
        p.apply("CreateValuesBytes", Create.of(testValues))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    input.apply(PubsubIO.writeMessages().to("projects/project/topics/topic"));
    p.run();

    AtomicBoolean sawPubsubOverride = new AtomicBoolean(false);
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(@UnknownKeyFor @NonNull @Initialized Node node) {
            if (node.getTransform() instanceof DataflowRunner.StreamingPubsubIOWrite) {
              sawPubsubOverride.set(true);
            }
          }
        });
    assertTrue(sawPubsubOverride.get());
  }

  @Test
  public void testBigQueryDLQWarningStreamingInsertsConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STREAMING_INSERTS, true);
  }

  @Test
  public void testBigQueryDLQWarningStreamingInsertsNotConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STREAMING_INSERTS, false);
  }

  @Test
  public void testBigQueryDLQWarningStorageApiConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STORAGE_WRITE_API, true);
  }

  @Test
  public void testBigQueryDLQWarningStorageApiNotConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STORAGE_WRITE_API, false);
  }

  @Test
  public void testBigQueryDLQWarningStorageApiALOConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE, true);
  }

  @Test
  public void testBigQueryDLQWarningStorageApiALONotConsumed() throws Exception {
    testBigQueryDLQWarning(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE, false);
  }

  public void testBigQueryDLQWarning(BigQueryIO.Write.Method method, boolean processFailures)
      throws IOException {
    PipelineOptions options = buildPipelineOptions();
    List<String> experiments =
        new ArrayList<>(ImmutableList.of(GcpOptions.STREAMING_ENGINE_EXPERIMENT));
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setExperiments(experiments);
    dataflowOptions.setStreaming(true);
    Pipeline p = Pipeline.create(options);

    List<TableRow> testValues = Arrays.asList(new TableRow(), new TableRow());
    PCollection<TableRow> input =
        p.apply("CreateValuesBytes", Create.of(testValues))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("project:dataset.table")
            .withSchema(new TableSchema())
            .withMethod(method)
            .withoutValidation();
    if (method == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      write = write.withAutoSharding().withTriggeringFrequency(Duration.standardSeconds(1));
    }
    WriteResult result = input.apply("BQWrite", write);
    if (processFailures) {
      if (method == BigQueryIO.Write.Method.STREAMING_INSERTS) {
        result
            .getFailedInserts()
            .apply(
                MapElements.into(TypeDescriptors.voids())
                    .via(SerializableFunctions.constant((Void) null)));
      } else {
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptors.voids())
                    .via(SerializableFunctions.constant((Void) null)));
      }
    }
    p.run();

    final String expectedWarning =
        "No transform processes the failed-inserts output from BigQuery sink: BQWrite!"
            + " Not processing failed inserts means that those rows will be lost.";
    if (processFailures) {
      expectedLogs.verifyNotLogged(expectedWarning);
    } else {
      expectedLogs.verifyWarn(expectedWarning);
    }
  }

  @Test
  public void testPubsubSinkDynamicOverride() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);
    Pipeline p = Pipeline.create(options);

    List<PubsubMessage> testValues =
        Arrays.asList(
            new PubsubMessage("foo".getBytes(StandardCharsets.UTF_8), Collections.emptyMap())
                .withTopic(""));
    PCollection<PubsubMessage> input =
        p.apply("CreateValuesBytes", Create.of(testValues))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    input.apply(PubsubIO.writeMessagesDynamic());
    p.run();

    AtomicBoolean sawPubsubOverride = new AtomicBoolean(false);
    p.traverseTopologically(
        new PipelineVisitor.Defaults() {

          @Override
          public void visitPrimitiveTransform(@UnknownKeyFor @NonNull @Initialized Node node) {
            if (node.getTransform() instanceof DataflowRunner.StreamingPubsubIOWrite) {
              sawPubsubOverride.set(true);
            }
          }
        });
    assertTrue(sawPubsubOverride.get());
  }

  static class TestExpansionServiceClientFactory implements ExpansionServiceClientFactory {
    ExpansionApi.ExpansionResponse response;

    @Override
    public ExpansionServiceClient getExpansionServiceClient(
        Endpoints.ApiServiceDescriptor endpoint) {
      return new ExpansionServiceClient() {
        @Override
        public ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
          Pipeline p = TestPipeline.create();
          p.apply(Create.of(1, 2, 3));
          SdkComponents sdkComponents =
              SdkComponents.create(p.getOptions()).withNewIdPrefix(request.getNamespace());
          RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents);
          String transformId = Iterables.getOnlyElement(pipelineProto.getRootTransformIdsList());
          RunnerApi.Components components = pipelineProto.getComponents();
          ImmutableList.Builder<String> requirementsBuilder = ImmutableList.builder();
          requirementsBuilder.addAll(pipelineProto.getRequirementsList());
          requirementsBuilder.add("ExternalTranslationTest_Requirement_URN");
          response =
              ExpansionApi.ExpansionResponse.newBuilder()
                  .setComponents(components)
                  .setTransform(
                      components
                          .getTransformsOrThrow(transformId)
                          .toBuilder()
                          .setUniqueName(transformId))
                  .addAllRequirements(requirementsBuilder.build())
                  .build();
          return response;
        }

        @Override
        public ExpansionApi.DiscoverSchemaTransformResponse discover(
            ExpansionApi.DiscoverSchemaTransformRequest request) {
          return null;
        }

        @Override
        public void close() throws Exception {
          // do nothing
        }
      };
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }
  }

  @Test
  public void testIsMultiLanguage() throws IOException {
    PipelineOptions options = buildPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> col =
        pipeline
            .apply(Create.of("1", "2", "3"))
            .apply(
                External.of(
                    "dummy_urn", new byte[] {}, "", new TestExpansionServiceClientFactory()));

    assertTrue(DataflowRunner.isMultiLanguagePipeline(pipeline));
  }

  private void testStreamingWriteOverride(PipelineOptions options, int expectedNumShards) {
    TestPipeline p = TestPipeline.fromOptions(options);

    StreamingShardedWriteFactory<Object, Void, Object> factory =
        new StreamingShardedWriteFactory<>(p.getOptions());
    WriteFiles<Object, Void, Object> original = WriteFiles.to(new TestSink(tmpFolder.toString()));
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));
    AppliedPTransform<PCollection<Object>, WriteFilesResult<Void>, WriteFiles<Object, Void, Object>>
        originalApplication =
            AppliedPTransform.of(
                "writefiles",
                PValues.expandInput(objs),
                Collections.emptyMap(),
                original,
                ResourceHints.create(),
                p);

    WriteFiles<Object, Void, Object> replacement =
        (WriteFiles<Object, Void, Object>)
            factory.getReplacementTransform(originalApplication).getTransform();
    assertThat(replacement, not(equalTo((Object) original)));
    assertThat(replacement.getNumShardsProvider().get(), equalTo(expectedNumShards));

    WriteFilesResult<Void> originalResult = objs.apply(original);
    WriteFilesResult<Void> replacementResult = objs.apply(replacement);
    Map<PCollection<?>, ReplacementOutput> res =
        factory.mapOutputs(PValues.expandOutput(originalResult), replacementResult);
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

                @Override
                public @Nullable ResourceId unwindowedFilename(
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
