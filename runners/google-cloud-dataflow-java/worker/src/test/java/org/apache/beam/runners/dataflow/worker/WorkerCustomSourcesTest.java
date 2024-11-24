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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Base64.decodeBase64;
import static org.apache.beam.runners.dataflow.internal.CustomSources.serializeToCloudSource;
import static org.apache.beam.runners.dataflow.util.Structs.getDictionary;
import static org.apache.beam.runners.dataflow.util.Structs.getObject;
import static org.apache.beam.runners.dataflow.util.Structs.getStrings;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.dictionaryToCloudSource;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static org.apache.beam.runners.dataflow.worker.WorkerCustomSources.BoundedReaderIterator.getReaderProgress;
import static org.apache.beam.runners.dataflow.worker.WorkerCustomSources.BoundedReaderIterator.longToParallelism;
import static org.apache.beam.sdk.testing.ExpectedLogs.verifyLogged;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.deserializeFromByteArray;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.DerivedSource;
import com.google.api.services.dataflow.model.DynamicSourceSplit;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ReportedParallelism;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitOptions;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.api.services.dataflow.model.Step;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.WorkerCustomSources.SplittableOnlyBoundedSource;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.config.FixedGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.testing.TestCountingSource;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.NativeReaderIterator;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WorkerCustomSources}. */
@RunWith(JUnit4.class)
public class WorkerCustomSourcesTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(WorkerCustomSources.class);

  private static final String COMPUTATION_ID = "computationId";

  private DataflowPipelineOptions options;

  @Before
  public void setUp() throws Exception {
    options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("TestAppName");
    options.setProject("test-project");
    options.setRegion("some-region1");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setRunner(DataflowRunner.class);
    options.setPathValidatorClass(NoopPathValidator.class);
  }

  @Test
  public void testSplitAndReadBundlesBack() throws Exception {
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(CountingSource.upTo(10L), options);
    List<WindowedValue<Integer>> elems = readElemsFromSource(options, source);
    assertEquals(10L, elems.size());
    for (long i = 0; i < 10L; i++) {
      assertEquals(valueInGlobalWindow(i), elems.get((int) i));
    }
    SourceSplitResponse response =
        performSplit(
            source,
            options,
            16L /*desiredBundleSizeBytes for two longs*/,
            null /* numBundles limit */,
            null /* API limit */);
    assertEquals("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED", response.getOutcome());
    List<DerivedSource> bundles = response.getBundles();
    assertEquals(5, bundles.size());
    for (int i = 0; i < 5; ++i) {
      DerivedSource bundle = bundles.get(i);
      assertEquals("SOURCE_DERIVATION_MODE_INDEPENDENT", bundle.getDerivationMode());
      com.google.api.services.dataflow.model.Source bundleSource = bundle.getSource();
      assertTrue(bundleSource.getDoesNotNeedSplitting());
      bundleSource.setCodec(source.getCodec());
      List<WindowedValue<Integer>> xs = readElemsFromSource(options, bundleSource);
      assertThat(
          "Failed on bundle " + i,
          xs,
          contains(valueInGlobalWindow(0L + 2 * i), valueInGlobalWindow(1L + 2 * i)));
      assertTrue(bundle.getSource().getMetadata().getEstimatedSizeBytes() > 0);
    }
  }

  private static Work createMockWork(Windmill.WorkItem workItem, Watermarks watermarks) {
    return Work.create(
        workItem,
        watermarks,
        Work.createProcessingContext(
            COMPUTATION_ID, new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class)),
        Instant::now,
        Collections.emptyList());
  }

  private static class SourceProducingSubSourcesInSplit extends MockSource {
    int numDesiredBundle;
    int sourceObjectSize;

    private transient @Nullable List<BoundedSource<Integer>> cachedSplitResult = null;

    public SourceProducingSubSourcesInSplit(int numDesiredBundle, int sourceObjectSize) {
      this.numDesiredBundle = numDesiredBundle;
      this.sourceObjectSize = sourceObjectSize;
    }

    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      if (cachedSplitResult == null) {
        ArrayList<SourceWithLargeObject> result = new ArrayList<>(numDesiredBundle);
        for (int i = 0; i < numDesiredBundle; ++i) {
          result.add(new SourceWithLargeObject(sourceObjectSize));
        }
        cachedSplitResult = ImmutableList.copyOf(result);
      }
      return cachedSplitResult;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return numDesiredBundle * 1000L;
    }
  }

  private static class SourceWithLargeObject extends MockSource {
    byte[] array;

    public SourceWithLargeObject(int sourceObjectSize) {
      byte[] array = new byte[sourceObjectSize];
      new Random().nextBytes(array);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 1000L;
    }
  }

  @Test
  public void testSplittingProducedResponseUnderLimit() throws Exception {
    SourceProducingSubSourcesInSplit source = new SourceProducingSubSourcesInSplit(200, 10_000);
    com.google.api.services.dataflow.model.Source cloudSource =
        translateIOToCloudSource(source, options);
    SourceSplitRequest splitRequest = new SourceSplitRequest();
    splitRequest.setSource(cloudSource);

    ExpectedLogs.LogSaver logSaver = new ExpectedLogs.LogSaver();
    LogManager.getLogManager()
        .getLogger("org.apache.beam.runners.dataflow.worker.WorkerCustomSources")
        .addHandler(logSaver);

    WorkerCustomSources.performSplitWithApiLimit(splitRequest, options, 100, 10_000);
    // verify initial split is not valid
    verifyLogged(
        ExpectedLogs.matcher(Level.WARNING, "this is too large for the Google Cloud Dataflow API"),
        logSaver);
    // verify that re-bundle is effective
    verifyLogged(ExpectedLogs.matcher(Level.WARNING, "Re-bundle source"), logSaver);
  }

  private static class SourceProducingNegativeEstimatedSizes extends MockSource {

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return -100;
    }

    @Override
    public String toString() {
      return "Some description";
    }
  }

  @Test
  public void testNegativeEstimatedSizesNotSet() throws Exception {
    WorkerCustomSources.BoundedSourceSplit<Integer> boundedSourceSplit =
        new WorkerCustomSources.BoundedSourceSplit<Integer>(
            new SourceProducingNegativeEstimatedSizes(),
            new SourceProducingNegativeEstimatedSizes());
    DynamicSourceSplit dynamicSourceSplit = WorkerCustomSources.toSourceSplit(boundedSourceSplit);
    assertNull(dynamicSourceSplit.getPrimary().getSource().getMetadata().getEstimatedSizeBytes());
    assertNull(dynamicSourceSplit.getResidual().getSource().getMetadata().getEstimatedSizeBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProgressAndSourceSplitTranslation() throws Exception {
    // Same as previous test, but now using BasicSerializableSourceFormat wrappers.
    // We know that the underlying reader behaves correctly (because of the previous test),
    // now check that we are wrapping it correctly.
    NativeReader<WindowedValue<Integer>> reader =
        (NativeReader<WindowedValue<Integer>>)
            ReaderRegistry.defaultRegistry()
                .create(
                    translateIOToCloudSource(CountingSource.upTo(10), options),
                    options,
                    null, // executionContext
                    TestOperationContext.create());
    try (NativeReader.NativeReaderIterator<WindowedValue<Integer>> iterator = reader.iterator()) {
      assertTrue(iterator.start());
      assertEquals(valueInGlobalWindow(0L), iterator.getCurrent());
      assertEquals(
          0.0,
          readerProgressToCloudProgress(iterator.getProgress()).getFractionConsumed().doubleValue(),
          1e-6);
      assertTrue(iterator.advance());
      assertEquals(valueInGlobalWindow(1L), iterator.getCurrent());
      assertEquals(
          0.1,
          readerProgressToCloudProgress(iterator.getProgress()).getFractionConsumed().doubleValue(),
          1e-6);
      assertTrue(iterator.advance());
      assertEquals(valueInGlobalWindow(2L), iterator.getCurrent());

      assertNull(iterator.requestDynamicSplit(ReaderTestUtils.splitRequestAtFraction(0)));
      assertNull(iterator.requestDynamicSplit(ReaderTestUtils.splitRequestAtFraction(0.1f)));
      WorkerCustomSources.BoundedSourceSplit<Integer> sourceSplit =
          (WorkerCustomSources.BoundedSourceSplit<Integer>)
              iterator.requestDynamicSplit(ReaderTestUtils.splitRequestAtFraction(0.5f));
      assertNotNull(sourceSplit);
      assertThat(readFromSource(sourceSplit.primary, options), contains(0L, 1L, 2L, 3L, 4L));
      assertThat(readFromSource(sourceSplit.residual, options), contains(5L, 6L, 7L, 8L, 9L));

      sourceSplit =
          (WorkerCustomSources.BoundedSourceSplit<Integer>)
              iterator.requestDynamicSplit(ReaderTestUtils.splitRequestAtFraction(0.8f));
      assertNotNull(sourceSplit);
      assertThat(readFromSource(sourceSplit.primary, options), contains(0L, 1L, 2L, 3L));
      assertThat(readFromSource(sourceSplit.residual, options), contains(4L));

      assertTrue(iterator.advance());
      assertEquals(valueInGlobalWindow(3L), iterator.getCurrent());
      assertFalse(iterator.advance());
    }
  }

  /**
   * A source that cannot do anything. Intended to be overridden for testing of individual methods.
   */
  private static class MockSource extends BoundedSource<Integer> {
    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Arrays.asList(this);
    }

    @Override
    public void validate() {}

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "<unknown>";
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return BigEndianIntegerCoder.of();
    }
  }

  private static class SourceProducingInvalidSplits extends MockSource {
    private String description;
    private String errorMessage;

    private SourceProducingInvalidSplits(String description, String errorMessage) {
      this.description = description;
      this.errorMessage = errorMessage;
    }

    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      Preconditions.checkState(errorMessage == null, "Unexpected invalid source");
      return Arrays.asList(
          new SourceProducingInvalidSplits("goodBundle", null),
          new SourceProducingInvalidSplits("badBundle", "intentionally invalid"));
    }

    @Override
    public void validate() {
      Preconditions.checkState(errorMessage == null, errorMessage);
    }

    @Override
    public String toString() {
      return description;
    }
  }

  @Test
  public void testSplittingProducedInvalidSource() throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        translateIOToCloudSource(new SourceProducingInvalidSplits("original", null), options);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        allOf(
            containsString("Splitting a valid source produced an invalid source"),
            containsString("original"),
            containsString("badBundle")));
    expectedException.expectCause(hasMessage(containsString("intentionally invalid")));
    performSplit(
        cloudSource,
        options,
        null /*desiredBundleSizeBytes*/,
        null /* numBundles limit */,
        null /* API limit */);
  }

  private static class FailingReader extends BoundedSource.BoundedReader<Integer> {
    private BoundedSource<Integer> source;

    private FailingReader(BoundedSource<Integer> source) {
      this.source = source;
    }

    @Override
    public BoundedSource<Integer> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      throw new IOException("Intentional error");
    }

    @Override
    public boolean advance() throws IOException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Double getFractionConsumed() {
      return null;
    }

    @Override
    public BoundedSource<Integer> splitAtFraction(double fraction) {
      return null;
    }
  }

  private static class SourceProducingFailingReader extends MockSource {
    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      return new FailingReader(this);
    }

    @Override
    public String toString() {
      return "Some description";
    }
  }

  @Test
  public void testFailureToStartReadingIncludesSourceDetails() throws Exception {
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(new SourceProducingFailingReader(), options);
    // Unfortunately Hamcrest doesn't have a matcher that can match on the exception's
    // printStackTrace(), however we just want to verify that the error and source description
    // would be contained in the exception *somewhere*, not necessarily in the top-level
    // Exception object. So instead we use Throwables.getStackTraceAsString and match on that.
    try {
      readElemsFromSource(options, source);
      fail("Expected to fail");
    } catch (Exception e) {
      assertThat(
          getStackTraceAsString(e),
          allOf(containsString("Intentional error"), containsString("Some description")));
    }
  }

  static com.google.api.services.dataflow.model.Source translateIOToCloudSource(
      BoundedSource<?> io, DataflowPipelineOptions options) throws Exception {
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline p = Pipeline.create(options);
    p.begin().apply(Read.from(io));

    // Note that we specifically perform this replacement since this is what the DataflowRunner
    // does and the DataflowRunner class does not expose a way to perform these replacements
    // without running the pipeline.
    p.replaceAll(Collections.singletonList(SplittableParDo.PRIMITIVE_BOUNDED_READ_OVERRIDE));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.Environment defaultEnvironmentForDataflow =
        Environments.createDockerEnvironment("dummy-image-url");
    sdkComponents.registerEnvironment(defaultEnvironmentForDataflow);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents, true);

    Job workflow =
        translator
            .translate(p, pipelineProto, sdkComponents, runner, new ArrayList<DataflowPackage>())
            .getJob();
    Step step = workflow.getSteps().get(0);

    return stepToCloudSource(step);
  }

  private static com.google.api.services.dataflow.model.Source stepToCloudSource(Step step)
      throws Exception {
    com.google.api.services.dataflow.model.Source res =
        dictionaryToCloudSource(
            getDictionary(step.getProperties(), PropertyNames.SOURCE_STEP_INPUT));
    // Encoding is specified in the step, not in the source itself.  This is
    // normal: incoming Dataflow API Source objects in map tasks will have the
    // encoding filled in from the step's output encoding.
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> outputInfo =
        (List<Map<String, Object>>) step.getProperties().get(PropertyNames.OUTPUT_INFO);

    CloudObject encoding =
        CloudObject.fromSpec(getObject(outputInfo.get(0), PropertyNames.ENCODING));
    res.setCodec(encoding);
    return res;
  }

  static SourceSplitResponse performSplit(
      com.google.api.services.dataflow.model.Source source,
      PipelineOptions options,
      @Nullable Long desiredBundleSizeBytes,
      @Nullable Integer numBundlesLimitForTest,
      @Nullable Long apiByteLimitForTest)
      throws Exception {
    SourceSplitRequest splitRequest = new SourceSplitRequest();
    splitRequest.setSource(source);
    if (desiredBundleSizeBytes != null) {
      splitRequest.setOptions(
          new SourceSplitOptions().setDesiredBundleSizeBytes(desiredBundleSizeBytes));
    }
    SourceOperationResponse response =
        WorkerCustomSources.performSplitWithApiLimit(
            splitRequest,
            options,
            MoreObjects.firstNonNull(
                numBundlesLimitForTest, WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT),
            MoreObjects.firstNonNull(
                apiByteLimitForTest, WorkerCustomSources.DATAFLOW_SPLIT_RESPONSE_API_SIZE_LIMIT));
    return response.getSplit();
  }

  @Test
  public void testUnboundedSplits() throws Exception {
    com.google.api.services.dataflow.model.Source source =
        serializeToCloudSource(new TestCountingSource(Integer.MAX_VALUE), options);
    List<String> serializedSplits =
        getStrings(source.getSpec(), WorkerCustomSources.SERIALIZED_SOURCE_SPLITS, null);
    assertEquals(20, serializedSplits.size());
    for (String serializedSplit : serializedSplits) {
      assertTrue(
          deserializeFromByteArray(decodeBase64(serializedSplit), "source")
              instanceof TestCountingSource);
    }
  }

  @Test
  public void testReadUnboundedReader() throws Exception {
    CounterSet counterSet = new CounterSet();
    StreamingModeExecutionStateRegistry executionStateRegistry =
        new StreamingModeExecutionStateRegistry();
    ReaderCache readerCache = new ReaderCache(Duration.standardMinutes(1), Runnable::run);
    StreamingGlobalConfigHandle globalConfigHandle =
        new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build());
    StreamingModeExecutionContext context =
        new StreamingModeExecutionContext(
            counterSet,
            COMPUTATION_ID,
            readerCache,
            /*stateNameMap=*/ ImmutableMap.of(),
            /*stateCache=*/ null,
            StreamingStepMetricsContainer.createRegistry(),
            new DataflowExecutionStateTracker(
                ExecutionStateSampler.newForTest(),
                executionStateRegistry.getState(
                    NameContext.forStage("stageName"), "other", null, NoopProfileScope.NOOP),
                counterSet,
                PipelineOptionsFactory.create(),
                "test-work-item-id"),
            executionStateRegistry,
            globalConfigHandle,
            Long.MAX_VALUE,
            /*throwExceptionOnLargeOutput=*/ false);

    options.setNumWorkers(5);
    int maxElements = 10;
    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    debugOptions.setUnboundedReaderMaxElements(maxElements);

    ByteString state = ByteString.EMPTY;
    for (int i = 0; i < 10 * maxElements;
    /* Incremented in inner loop */ ) {
      // Initialize streaming context with state from previous iteration.
      context.start(
          "key",
          createMockWork(
              Windmill.WorkItem.newBuilder()
                  .setKey(ByteString.copyFromUtf8("0000000000000001")) // key is zero-padded index.
                  .setWorkToken(i) // Must be increasing across activations for cache to be used.
                  .setCacheToken(1)
                  .setSourceState(
                      Windmill.SourceState.newBuilder().setState(state).build()) // Source state.
                  .build(),
              Watermarks.builder().setInputDataWatermark(new Instant(0)).build()),
          mock(WindmillStateReader.class),
          mock(SideInputStateFetcher.class),
          Windmill.WorkItemCommitRequest.newBuilder());

      @SuppressWarnings({"unchecked", "rawtypes"})
      NativeReader<WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>> reader =
          (NativeReader)
              WorkerCustomSources.create(
                  (CloudObject)
                      serializeToCloudSource(new TestCountingSource(Integer.MAX_VALUE), options)
                          .getSpec(),
                  options,
                  context);

      // Verify data.
      Instant beforeReading = Instant.now();
      int numReadOnThisIteration = 0;
      for (WindowedValue<ValueWithRecordId<KV<Integer, Integer>>> value :
          ReaderUtils.readAllFromReader(reader)) {
        assertEquals(KV.of(0, i), value.getValue().getValue());
        assertArrayEquals(
            encodeToByteArray(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()), KV.of(0, i)),
            value.getValue().getId());
        assertThat(value.getWindows(), contains((BoundedWindow) GlobalWindow.INSTANCE));
        assertEquals(i, value.getTimestamp().getMillis());
        i++;
        numReadOnThisIteration++;
      }
      Instant afterReading = Instant.now();
      long maxReadSec = debugOptions.getUnboundedReaderMaxReadTimeSec();
      assertThat(
          new Duration(beforeReading, afterReading).getStandardSeconds(),
          lessThanOrEqualTo(maxReadSec + 1));
      assertThat(
          numReadOnThisIteration, lessThanOrEqualTo(debugOptions.getUnboundedReaderMaxElements()));

      // Extract and verify state modifications.
      context.flushState();
      state = context.getOutputBuilder().getSourceStateUpdates().getState();
      // CountingSource's watermark is the last record + 1.  i is now one past the last record,
      // so the expected watermark is i millis.
      assertEquals(
          TimeUnit.MILLISECONDS.toMicros(i), context.getOutputBuilder().getSourceWatermark());
      assertEquals(
          1, context.getOutputBuilder().getSourceStateUpdates().getFinalizeIdsList().size());

      assertNotNull(
          readerCache.acquireReader(
              context.getComputationKey(),
              context.getWorkItem().getCacheToken(),
              context.getWorkToken() + 1));
      assertEquals(7L, context.getBacklogBytes());
    }
  }

  @Test
  public void testLargeSerializedSizeResplits() throws Exception {
    final long apiSizeLimitForTest = 5 * 1024;
    // Figure out how many splits of CountingSource are needed to exceed the API limits, using an
    // extra factor of 2 to ensure that we go over the limits.
    BoundedSource<Long> justForSizing = CountingSource.upTo(1000000L);
    long size =
        DataflowApiUtils.computeSerializedSizeBytes(
            translateIOToCloudSource(justForSizing, options));
    long numberToSplitToExceedLimit = 2 * apiSizeLimitForTest / size;
    checkState(
        numberToSplitToExceedLimit < WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT,
        "This test expects the number of splits to be less than %s "
            + "to avoid using SplittableOnlyBoundedSource",
        WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT);

    // Generate a CountingSource and split it into the desired number of splits
    // (desired size = 8 bytes, 1 long), triggering the re-split with a larger bundle size.
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(CountingSource.upTo(numberToSplitToExceedLimit), options);
    SourceSplitResponse split =
        performSplit(source, options, 8L, null /* numBundles limit */, apiSizeLimitForTest);
    logged.verifyWarn("too large for the Google Cloud Dataflow API");
    logged.verifyWarn(String.format("%d bundles", numberToSplitToExceedLimit));
    assertThat((long) split.getBundles().size(), lessThan(numberToSplitToExceedLimit));
  }

  @Test
  public void testLargeNumberOfSplitsReturnsSplittableOnlyBoundedSources() throws Exception {
    final long apiSizeLimitForTest = 500 * 1024;
    // Generate a CountingSource and split it into the desired number of splits
    // (desired size = 1 byte), triggering the re-split with a larger bundle size.
    // Thus below we expect to produce 451 splits.
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(CountingSource.upTo(451), options);
    SourceSplitResponse split =
        performSplit(source, options, 1L, null /* numBundles limit */, apiSizeLimitForTest);
    assertEquals(WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT, split.getBundles().size());

    // We expect that we would have the 100 splits that were generated from the initial
    // splitting done by CountingSource. The splits should encompass the counting sources for
    // 0-99, 100-199, 200-299, 300-355, 356, 357, ... 451
    for (int i = 0; i <= 3; ++i) {
      DerivedSource derivedSource = split.getBundles().get(i);
      // Make sure that we are setting the flag telling Dataflow that we need further splits.
      assertFalse(derivedSource.getSource().getDoesNotNeedSplitting());
      Object deserializedSource =
          WorkerCustomSources.deserializeFromCloudSource(derivedSource.getSource().getSpec());
      assertTrue(deserializedSource instanceof SplittableOnlyBoundedSource);
      SplittableOnlyBoundedSource<?> splittableOnlySource =
          (SplittableOnlyBoundedSource<?>) deserializedSource;

      List<? extends BoundedSource<?>> splitSources = splittableOnlySource.split(1L, options);
      int expectedNumSplits = i < 3 ? 100 : 55;
      assertEquals(expectedNumSplits, splitSources.size());
      for (int j = 0; j < splitSources.size(); ++j) {
        assertTrue(splitSources.get(j) instanceof OffsetBasedSource);
        OffsetBasedSource<?> offsetBasedSource = (OffsetBasedSource<?>) splitSources.get(j);
        assertEquals(i * 100 + j, offsetBasedSource.getStartOffset());
        assertEquals(i * 100 + j + 1, offsetBasedSource.getEndOffset());
      }
    }

    for (int i = 4; i < WorkerCustomSources.DEFAULT_NUM_BUNDLES_LIMIT; ++i) {
      DerivedSource derivedSource = split.getBundles().get(i);
      // Make sure that we are not setting the flag telling Dataflow that we need further splits
      // for the individual counting sources
      assertTrue(derivedSource.getSource().getDoesNotNeedSplitting());
      Object deserializedSource =
          WorkerCustomSources.deserializeFromCloudSource(derivedSource.getSource().getSpec());
      assertTrue(deserializedSource instanceof OffsetBasedSource);
      OffsetBasedSource<?> offsetBasedSource = (OffsetBasedSource<?>) deserializedSource;
      assertEquals(351 + i, offsetBasedSource.getStartOffset());
      assertEquals(351 + i + 1, offsetBasedSource.getEndOffset());
    }
  }

  @Test
  public void testOversplittingDesiredBundleSizeScaledFirst() throws Exception {
    // Create a source that greatly oversplits but with coalescing/compression it would still fit
    // under the API limit. Test that the API limit gets applied first, so oversplitting is
    // reduced.
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(CountingSource.upTo(8000), options);

    // Without either limit, produces 1000 bundles, total size ~500kb.
    // With only numBundles limit 100, produces 100 bundles, total size ~72kb.
    // With only apiSize limit = 10kb, 72 bundles, total size ~40kb (over the limit but oh well).
    // With numBundles limit 100 and apiSize limit 10kb, should produce 72 bundles.
    // On the other hand, if the numBundles limit of 100 was applied first, we'd get 100 bundles.

    SourceSplitResponse bundledWithOnlyNumBundlesLimit =
        performSplit(
            source, options, 8L, 100 /* numBundles limit */, 10000 * 1024L /* API size limit */);
    assertEquals(100, bundledWithOnlyNumBundlesLimit.getBundles().size());
    assertThat(
        DataflowApiUtils.computeSerializedSizeBytes(bundledWithOnlyNumBundlesLimit),
        greaterThan(10 * 1024L));

    SourceSplitResponse bundledWithOnlySizeLimit =
        performSplit(
            source, options, 8L, 1000000 /* numBundles limit */, 10 * 1024L /* API size limit */);
    int numBundlesWithOnlySizeLimit = bundledWithOnlySizeLimit.getBundles().size();
    assertThat(numBundlesWithOnlySizeLimit, lessThan(100));

    SourceSplitResponse bundledWithSizeLimit = performSplit(source, options, 8L, 100, 10 * 1024L);
    assertEquals(numBundlesWithOnlySizeLimit, bundledWithSizeLimit.getBundles().size());
  }

  @Test
  public void testTooLargeSplitResponseFails() throws Exception {
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(CountingSource.upTo(1000), options);

    expectedException.expectMessage("[0, 1000)");
    expectedException.expectMessage("larger than the limit 100");
    performSplit(source, options, 8L, 10, 100L);
  }

  /**
   * Creates a {@link NativeReader} from the given Dataflow Source API definition and reads all
   * elements from it.
   */
  public static <T> List<T> readElemsFromSource(PipelineOptions options, Source source) {
    try {
      @SuppressWarnings("unchecked")
      NativeReader<T> reader =
          (NativeReader<T>)
              ReaderRegistry.defaultRegistry()
                  .create(source, options, null, TestOperationContext.create());
      return ReaderUtils.readAllFromReader(reader);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from source: " + source.toString(), e);
    }
  }

  private static class TestBoundedReader extends BoundedReader<Void> {
    private final @Nullable Object fractionConsumed;
    private final Object splitPointsConsumed;
    private final Object splitPointsRemaining;

    public TestBoundedReader(
        @Nullable Object fractionConsumed,
        Object splitPointsConsumed,
        Object splitPointsRemaining) {
      this.fractionConsumed = fractionConsumed;
      this.splitPointsConsumed = splitPointsConsumed;
      this.splitPointsRemaining = splitPointsRemaining;
    }

    @Override
    public BoundedSource<Void> getCurrentSource() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean start() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advance() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void getCurrent() throws NoSuchElementException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Double getFractionConsumed() {
      if (fractionConsumed instanceof Number || fractionConsumed == null) {
        return ((Number) fractionConsumed).doubleValue();
      } else {
        throw (RuntimeException) fractionConsumed;
      }
    }

    @Override
    public long getSplitPointsConsumed() {
      if (splitPointsConsumed instanceof Number) {
        return ((Number) splitPointsConsumed).longValue();
      } else {
        throw (RuntimeException) splitPointsConsumed;
      }
    }

    @Override
    public long getSplitPointsRemaining() {
      if (splitPointsRemaining instanceof Number) {
        return ((Number) splitPointsRemaining).longValue();
      } else {
        throw (RuntimeException) splitPointsRemaining;
      }
    }
  }

  @Test
  public void testLongToParallelism() {
    // Invalid values should return null
    assertNull(longToParallelism(-10));
    assertNull(longToParallelism(-1));

    // Valid values should be finite and non-negative
    ReportedParallelism p = longToParallelism(0);
    assertEquals(p.getValue(), 0.0, 1e-6);

    p = longToParallelism(100);
    assertEquals(p.getValue(), 100.0, 1e-6);

    p = longToParallelism(Long.MAX_VALUE);
    assertEquals(p.getValue(), Long.MAX_VALUE, 1e-6);
  }

  @Test
  public void testGetReaderProgress() {
    ApproximateReportedProgress progress = getReaderProgress(new TestBoundedReader(0.75, 1, 2));
    assertEquals(0.75, progress.getFractionConsumed(), 1e-6);
    assertEquals(1.0, progress.getConsumedParallelism().getValue(), 1e-6);
    assertEquals(2.0, progress.getRemainingParallelism().getValue(), 1e-6);

    progress = getReaderProgress(new TestBoundedReader(null, -1, 4));
    assertNull(progress.getFractionConsumed());
    assertNull(progress.getConsumedParallelism());
    assertEquals(4.0, progress.getRemainingParallelism().getValue(), 1e-6);

    progress = getReaderProgress(new TestBoundedReader(null, -1, -2));
    assertNull(progress.getFractionConsumed());
    assertNull(progress.getConsumedParallelism());
    assertNull(progress.getRemainingParallelism());
  }

  @Test
  public void testGetReaderProgressThrowing() {
    // Fraction throws, remaining and consumed still okay.
    RuntimeException fractionError = new UnsupportedOperationException("fraction");
    ApproximateReportedProgress progress =
        getReaderProgress(new TestBoundedReader(fractionError, 1, 2));
    assertNull(progress.getFractionConsumed());
    assertEquals(1.0, progress.getConsumedParallelism().getValue(), 1e-6);
    assertEquals(2.0, progress.getRemainingParallelism().getValue(), 1e-6);
    logged.verifyWarn("fraction");

    // Consumed throws, fraction and remaining still okay.
    RuntimeException consumedError = new UnsupportedOperationException("consumed parallelism");
    progress = getReaderProgress(new TestBoundedReader(0.75, consumedError, 3));
    assertEquals(0.75, progress.getFractionConsumed(), 1e-6);
    assertNull(progress.getConsumedParallelism());
    assertEquals(3.0, progress.getRemainingParallelism().getValue(), 1e-6);
    logged.verifyWarn("consumed parallelism");

    // Remaining throws, consumed and remaining still okay.
    RuntimeException remainingError = new UnsupportedOperationException("remaining parallelism");
    progress = getReaderProgress(new TestBoundedReader(0.5, 5, remainingError));
    assertEquals(0.5, progress.getFractionConsumed(), 1e-6);
    assertEquals(5.0, progress.getConsumedParallelism().getValue(), 1e-6);
    assertNull(progress.getRemainingParallelism());
    logged.verifyWarn("remaining parallelism");
  }

  @Test
  public void testFailedWorkItemsAbort() throws Exception {
    CounterSet counterSet = new CounterSet();
    StreamingModeExecutionStateRegistry executionStateRegistry =
        new StreamingModeExecutionStateRegistry();
    StreamingGlobalConfigHandle globalConfigHandle =
        new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build());
    StreamingModeExecutionContext context =
        new StreamingModeExecutionContext(
            counterSet,
            COMPUTATION_ID,
            new ReaderCache(Duration.standardMinutes(1), Runnable::run),
            /*stateNameMap=*/ ImmutableMap.of(),
            WindmillStateCache.builder()
                .setSizeMb(options.getWorkerCacheMb())
                .build()
                .forComputation(COMPUTATION_ID),
            StreamingStepMetricsContainer.createRegistry(),
            new DataflowExecutionStateTracker(
                ExecutionStateSampler.newForTest(),
                executionStateRegistry.getState(
                    NameContext.forStage("stageName"), "other", null, NoopProfileScope.NOOP),
                counterSet,
                PipelineOptionsFactory.create(),
                "test-work-item-id"),
            executionStateRegistry,
            globalConfigHandle,
            Long.MAX_VALUE,
            /*throwExceptionOnLargeOutput=*/ false);

    options.setNumWorkers(5);
    int maxElements = 100;
    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    debugOptions.setUnboundedReaderMaxElements(maxElements);

    ByteString state = ByteString.EMPTY;
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("0000000000000001")) // key is zero-padded index.
            .setWorkToken(0)
            .setCacheToken(1)
            .setSourceState(
                Windmill.SourceState.newBuilder().setState(state).build()) // Source state.
            .build();
    Work dummyWork =
        Work.create(
            workItem,
            Watermarks.builder().setInputDataWatermark(new Instant(0)).build(),
            Work.createProcessingContext(
                COMPUTATION_ID,
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            Instant::now,
            Collections.emptyList());
    context.start(
        "key",
        dummyWork,
        mock(WindmillStateReader.class),
        mock(SideInputStateFetcher.class),
        Windmill.WorkItemCommitRequest.newBuilder());

    @SuppressWarnings({"unchecked", "rawtypes"})
    NativeReader<WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>> reader =
        (NativeReader)
            WorkerCustomSources.create(
                (CloudObject)
                    serializeToCloudSource(new TestCountingSource(Integer.MAX_VALUE), options)
                        .getSpec(),
                options,
                context);

    NativeReaderIterator<WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>> readerIterator =
        reader.iterator();
    int numReads = 0;
    while ((numReads == 0) ? readerIterator.start() : readerIterator.advance()) {
      WindowedValue<ValueWithRecordId<KV<Integer, Integer>>> value = readerIterator.getCurrent();
      assertEquals(KV.of(0, numReads), value.getValue().getValue());
      numReads++;
      // Fail the work item after reading two elements.
      if (numReads == 2) {
        dummyWork.setFailed();
      }
    }
    assertThat(numReads, equalTo(2));
  }
}
