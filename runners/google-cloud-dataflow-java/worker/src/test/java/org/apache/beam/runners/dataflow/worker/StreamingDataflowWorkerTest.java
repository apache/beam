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

import static org.apache.beam.runners.dataflow.util.Structs.addObject;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.splitIntToLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Sink;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandleImpl;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.runners.dataflow.worker.testing.TestCountingSource;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.InputMessageBundle;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedMessageBundle;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.State;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer.Type;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WatermarkHold;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactoryFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedLong;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link StreamingDataflowWorker}. */
@RunWith(Parameterized.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings({"unused", "deprecation"})
public class StreamingDataflowWorkerTest {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorkerTest.class);
  private static final IntervalWindow DEFAULT_WINDOW =
      new IntervalWindow(new Instant(1234), Duration.millis(1000));
  private static final IntervalWindow WINDOW_AT_ZERO =
      new IntervalWindow(new Instant(0), new Instant(1000));
  private static final IntervalWindow WINDOW_AT_ONE_SECOND =
      new IntervalWindow(new Instant(1000), new Instant(2000));
  private static final Coder<IntervalWindow> DEFAULT_WINDOW_CODER = IntervalWindow.getCoder();
  private static final Coder<Collection<IntervalWindow>> DEFAULT_WINDOW_COLLECTION_CODER =
      CollectionCoder.of(DEFAULT_WINDOW_CODER);
  // Default values that are unimportant for correctness, but must be consistent
  // between pieces of this test suite
  private static final String DEFAULT_COMPUTATION_ID = "computation";
  private static final String DEFAULT_MAP_STAGE_NAME = "computation";
  private static final String DEFAULT_MAP_SYSTEM_NAME = "computation";
  private static final String DEFAULT_OUTPUT_ORIGINAL_NAME = "originalName";
  private static final String DEFAULT_OUTPUT_SYSTEM_NAME = "systemName";
  private static final String DEFAULT_PARDO_SYSTEM_NAME = "parDo";
  private static final String DEFAULT_PARDO_ORIGINAL_NAME = "parDoOriginalName";
  private static final String DEFAULT_PARDO_USER_NAME = "parDoUserName";
  private static final String DEFAULT_PARDO_STATE_FAMILY = "parDoStateFamily";
  private static final String DEFAULT_SOURCE_SYSTEM_NAME = "source";
  private static final String DEFAULT_SOURCE_ORIGINAL_NAME = "sourceOriginalName";
  private static final String DEFAULT_SINK_SYSTEM_NAME = "sink";
  private static final String DEFAULT_SINK_ORIGINAL_NAME = "sinkOriginalName";
  private static final String DEFAULT_SOURCE_COMPUTATION_ID = "upstream";
  private static final String DEFAULT_KEY_STRING = "key";
  private static final long DEFAULT_SHARDING_KEY = 12345;
  private static final ByteString DEFAULT_KEY_BYTES = ByteString.copyFromUtf8(DEFAULT_KEY_STRING);
  private static final String DEFAULT_DATA_STRING = "data";
  private static final String DEFAULT_DESTINATION_STREAM_ID = "out";
  private static final long MAXIMUM_BYTES_OUTSTANDING = 10000000;
  private static final Function<GetDataRequest, GetDataResponse> EMPTY_DATA_RESPONDER =
      (GetDataRequest request) -> {
        GetDataResponse.Builder builder = GetDataResponse.newBuilder();
        for (ComputationGetDataRequest compRequest : request.getRequestsList()) {
          ComputationGetDataResponse.Builder compBuilder =
              builder.addDataBuilder().setComputationId(compRequest.getComputationId());
          for (KeyedGetDataRequest keyRequest : compRequest.getRequestsList()) {
            KeyedGetDataResponse.Builder keyBuilder =
                compBuilder
                    .addDataBuilder()
                    .setKey(keyRequest.getKey())
                    .setShardingKey(keyRequest.getShardingKey());
            keyBuilder.addAllValues(keyRequest.getValuesToFetchList());
            keyBuilder.addAllBags(keyRequest.getBagsToFetchList());
            keyBuilder.addAllWatermarkHolds(keyRequest.getWatermarkHoldsToFetchList());
          }
        }
        return builder.build();
      };
  private final boolean streamingEngine;
  private final Supplier<Long> idGenerator =
      new Supplier<Long>() {
        private final AtomicLong idGenerator = new AtomicLong(1L);

        @Override
        public Long get() {
          return idGenerator.getAndIncrement();
        }
      };

  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Rule public BlockingFn blockingFn = new BlockingFn();
  @Rule public TestRule restoreMDC = new RestoreDataflowLoggingMDC();
  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  WorkUnitClient mockWorkUnitClient = mock(WorkUnitClient.class);
  StreamingGlobalConfigHandleImpl mockGlobalConfigHandle =
      mock(StreamingGlobalConfigHandleImpl.class);
  HotKeyLogger hotKeyLogger = mock(HotKeyLogger.class);

  private @Nullable ComputationStateCache computationStateCache = null;
  private final FakeWindmillServer server =
      new FakeWindmillServer(
          errorCollector, computationId -> computationStateCache.get(computationId));
  private StreamingCounters streamingCounters;

  public StreamingDataflowWorkerTest(Boolean streamingEngine) {
    this.streamingEngine = streamingEngine;
  }

  @Parameterized.Parameters(name = "{index}: [streamingEngine={0}]")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private static CounterUpdate getCounter(Iterable<CounterUpdate> counters, String name) {
    for (CounterUpdate counter : counters) {
      if (counter.getNameAndKind().getName().equals(name)) {
        return counter;
      }
    }
    return null;
  }

  private Iterable<CounterUpdate> buildCounters() {
    return Iterables.concat(
        streamingCounters
            .pendingDeltaCounters()
            .extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE),
        streamingCounters
            .pendingCumulativeCounters()
            .extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
  }

  @Before
  public void setUp() {
    server.clearCommitsReceived();
    streamingCounters = StreamingCounters.create();
  }

  @After
  public void cleanUp() {
    Optional.ofNullable(computationStateCache)
        .ifPresent(ComputationStateCache::closeAndInvalidateAll);
  }

  private static ExecutableWork createMockWork(
      ShardedKey shardedKey, long workToken, String computationId) {
    return createMockWork(shardedKey, workToken, computationId, ignored -> {});
  }

  private static ExecutableWork createMockWork(
      ShardedKey shardedKey, long workToken, Consumer<Work> processWorkFn) {
    return createMockWork(shardedKey, workToken, "computationId", processWorkFn);
  }

  private static ExecutableWork createMockWork(
      ShardedKey shardedKey, long workToken, String computationId, Consumer<Work> processWorkFn) {
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder()
                .setKey(shardedKey.key())
                .setShardingKey(shardedKey.shardingKey())
                .setWorkToken(workToken)
                .build(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                computationId, new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class)),
            Instant::now,
            Collections.emptyList()),
        processWorkFn);
  }

  private byte[] intervalWindowBytes(IntervalWindow window) throws Exception {
    return CoderUtils.encodeToByteArray(
        DEFAULT_WINDOW_COLLECTION_CODER, Collections.singletonList(window));
  }

  /**
   * Add options with the following format: "--{OPTION_NAME}={OPTION_VALUE}" with 1 option flag per
   * String.
   *
   * <p>Example: {@code defaultWorkerParams("--option_a=1", "--option_b=foo", "--option_c=bar");}
   */
  private StreamingDataflowWorkerTestParams.Builder defaultWorkerParams(String... options) {
    return StreamingDataflowWorkerTestParams.builder()
        .setOptions(createTestingPipelineOptions(options));
  }

  private String keyStringForIndex(int index) {
    return DEFAULT_KEY_STRING + index;
  }

  private String dataStringForIndex(long index) {
    return DEFAULT_DATA_STRING + index;
  }

  private ParallelInstruction makeWindowingSourceInstruction(Coder<?> coder) {
    CloudObject timerCloudObject =
        CloudObject.forClassName(
            "com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder");
    List<CloudObject> component =
        Collections.singletonList(CloudObjects.asCloudObject(coder, /* sdkComponents= */ null));
    Structs.addList(timerCloudObject, PropertyNames.COMPONENT_ENCODINGS, component);

    CloudObject encodedCoder = CloudObject.forClassName("kind:windowed_value");
    Structs.addBoolean(encodedCoder, PropertyNames.IS_WRAPPER, true);
    Structs.addList(
        encodedCoder,
        PropertyNames.COMPONENT_ENCODINGS,
        ImmutableList.of(
            timerCloudObject,
            CloudObjects.asCloudObject(IntervalWindowCoder.of(), /* sdkComponents= */ null)));

    return new ParallelInstruction()
        .setSystemName(DEFAULT_SOURCE_SYSTEM_NAME)
        .setOriginalName(DEFAULT_SOURCE_ORIGINAL_NAME)
        .setRead(
            new ReadInstruction()
                .setSource(
                    new Source()
                        .setSpec(CloudObject.forClass(WindowingWindmillReader.class))
                        .setCodec(encodedCoder)))
        .setOutputs(
            Collections.singletonList(
                new InstructionOutput()
                    .setName(Long.toString(idGenerator.get()))
                    .setCodec(encodedCoder)
                    .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                    .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)));
  }

  private ParallelInstruction makeSourceInstruction(Coder<?> coder) {
    return new ParallelInstruction()
        .setSystemName(DEFAULT_SOURCE_SYSTEM_NAME)
        .setOriginalName(DEFAULT_SOURCE_ORIGINAL_NAME)
        .setRead(
            new ReadInstruction()
                .setSource(
                    new Source()
                        .setSpec(CloudObject.forClass(UngroupedWindmillReader.class))
                        .setCodec(
                            CloudObjects.asCloudObject(
                                WindowedValue.getFullCoder(coder, IntervalWindow.getCoder()),
                                /* sdkComponents= */ null))))
        .setOutputs(
            Collections.singletonList(
                new InstructionOutput()
                    .setName(Long.toString(idGenerator.get()))
                    .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                    .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                    .setCodec(
                        CloudObjects.asCloudObject(
                            WindowedValue.getFullCoder(coder, IntervalWindow.getCoder()),
                            /* sdkComponents= */ null))));
  }

  private ParallelInstruction makeDoFnInstruction(
      DoFn<?, ?> doFn,
      int producerIndex,
      Coder<?> outputCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    CloudObject spec = CloudObject.forClassName("DoFn");
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    doFn,
                    windowingStrategy /* windowing strategy */,
                    null /* side input views */,
                    null /* input coder */,
                    new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap()))));
    return new ParallelInstruction()
        .setSystemName(DEFAULT_PARDO_SYSTEM_NAME)
        .setName(DEFAULT_PARDO_USER_NAME)
        .setOriginalName(DEFAULT_PARDO_ORIGINAL_NAME)
        .setParDo(
            new ParDoInstruction()
                .setInput(
                    new InstructionInput()
                        .setProducerInstructionIndex(producerIndex)
                        .setOutputNum(0))
                .setNumOutputs(1)
                .setUserFn(spec)
                .setMultiOutputInfos(
                    Collections.singletonList(new MultiOutputInfo().setTag(PropertyNames.OUTPUT))))
        .setOutputs(
            Collections.singletonList(
                new InstructionOutput()
                    .setName(PropertyNames.OUTPUT)
                    .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                    .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                    .setCodec(
                        CloudObjects.asCloudObject(
                            WindowedValue.getFullCoder(
                                outputCoder, windowingStrategy.getWindowFn().windowCoder()),
                            /* sdkComponents= */ null))));
  }

  private ParallelInstruction makeDoFnInstruction(
      DoFn<?, ?> doFn, int producerIndex, Coder<?> outputCoder) {
    // The windows used in this test are actually fairly arbitrary and could not be assigned
    // by FixedWindow. However, the WindowingStrategy on the DoFnInfo is used only for
    // the window Coder, which is IntervalWindow.Coder
    WindowingStrategy<?, ?> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    return makeDoFnInstruction(doFn, producerIndex, outputCoder, windowingStrategy);
  }

  private ParallelInstruction makeSinkInstruction(
      String streamId,
      Coder<?> coder,
      int producerIndex,
      Coder<? extends BoundedWindow> windowCoder) {
    CloudObject spec = CloudObject.forClass(WindmillSink.class);
    addString(spec, "stream_id", streamId);
    return new ParallelInstruction()
        .setSystemName(DEFAULT_SINK_SYSTEM_NAME)
        .setOriginalName(DEFAULT_SINK_ORIGINAL_NAME)
        .setWrite(
            new WriteInstruction()
                .setInput(
                    new InstructionInput()
                        .setProducerInstructionIndex(producerIndex)
                        .setOutputNum(0))
                .setSink(
                    new Sink()
                        .setSpec(spec)
                        .setCodec(
                            CloudObjects.asCloudObject(
                                WindowedValue.getFullCoder(coder, windowCoder),
                                /* sdkComponents= */ null))));
  }

  private ParallelInstruction makeSinkInstruction(
      Coder<?> coder, int producerIndex, Coder<? extends BoundedWindow> windowCoder) {
    return makeSinkInstruction(DEFAULT_DESTINATION_STREAM_ID, coder, producerIndex, windowCoder);
  }

  private ParallelInstruction makeSinkInstruction(Coder<?> coder, int producerIndex) {
    return makeSinkInstruction(coder, producerIndex, IntervalWindow.getCoder());
  }

  /**
   * Returns a {@link MapTask} with the provided {@code instructions} and default values everywhere
   * else.
   */
  private MapTask defaultMapTask(List<ParallelInstruction> instructions) {
    MapTask mapTask =
        new MapTask()
            .setStageName(DEFAULT_MAP_STAGE_NAME)
            .setSystemName(DEFAULT_MAP_SYSTEM_NAME)
            .setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());
    return mapTask;
  }

  private Windmill.GetWorkResponse buildInput(String input, byte[] metadata) throws Exception {
    Windmill.GetWorkResponse.Builder builder = Windmill.GetWorkResponse.newBuilder();
    TextFormat.merge(input, builder);
    if (metadata != null) {
      Windmill.InputMessageBundle.Builder messageBundleBuilder =
          builder.getWorkBuilder(0).getWorkBuilder(0).getMessageBundlesBuilder(0);
      for (Windmill.Message.Builder messageBuilder :
          messageBundleBuilder.getMessagesBuilderList()) {
        messageBuilder.setMetadata(addPaneTag(PaneInfo.NO_FIRING, metadata));
      }
    }

    return builder.build();
  }

  private Windmill.GetWorkResponse buildSessionInput(
      int workToken,
      long inputWatermark,
      long outputWatermark,
      List<Long> inputs,
      List<Timer> timers)
      throws Exception {
    Windmill.WorkItem.Builder builder = Windmill.WorkItem.newBuilder();
    builder.setKey(DEFAULT_KEY_BYTES);
    builder.setShardingKey(DEFAULT_SHARDING_KEY);
    builder.setCacheToken(1);
    builder.setWorkToken(workToken);
    builder.setOutputDataWatermark(outputWatermark * 1000);
    if (!inputs.isEmpty()) {
      InputMessageBundle.Builder messageBuilder =
          Windmill.InputMessageBundle.newBuilder()
              .setSourceComputationId(DEFAULT_SOURCE_COMPUTATION_ID);
      for (Long input : inputs) {
        messageBuilder.addMessages(
            Windmill.Message.newBuilder()
                .setTimestamp(input)
                .setData(ByteString.copyFromUtf8(dataStringForIndex(input)))
                .setMetadata(
                    addPaneTag(
                        PaneInfo.NO_FIRING,
                        intervalWindowBytes(
                            new IntervalWindow(
                                new Instant(input),
                                new Instant(input).plus(Duration.millis(10)))))));
      }
      builder.addMessageBundles(messageBuilder);
    }
    if (!timers.isEmpty()) {
      builder.setTimers(Windmill.TimerBundle.newBuilder().addAllTimers(timers));
    }
    return Windmill.GetWorkResponse.newBuilder()
        .addWork(
            Windmill.ComputationWorkItems.newBuilder()
                .setComputationId(DEFAULT_COMPUTATION_ID)
                .setInputDataWatermark(inputWatermark * 1000)
                .addWork(builder))
        .build();
  }

  private Windmill.GetWorkResponse makeInput(int index, long timestamp) throws Exception {
    return makeInput(index, timestamp, keyStringForIndex(index), DEFAULT_SHARDING_KEY);
  }

  private Windmill.GetWorkResponse makeInput(
      int index, long timestamp, String key, long shardingKey) throws Exception {
    return buildInput(
        "work {"
            + "  computation_id: \""
            + DEFAULT_COMPUTATION_ID
            + "\""
            + "  input_data_watermark: 0"
            + "  work {"
            + "    key: \""
            + key
            + "\""
            + "    sharding_key: "
            + shardingKey
            + "    work_token: "
            + index
            + "    cache_token: "
            + (index + 1)
            + "    hot_key_info {"
            + "      hot_key_age_usec: 1000000"
            + "    }"
            + "    message_bundles {"
            + "      source_computation_id: \""
            + DEFAULT_SOURCE_COMPUTATION_ID
            + "\""
            + "      messages {"
            + "        timestamp: "
            + timestamp
            + "        data: \"data"
            + index
            + "\""
            + "      }"
            + "    }"
            + "  }"
            + "}",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Collections.singletonList(DEFAULT_WINDOW)));
  }

  private Windmill.GetWorkResponse makeInput(
      int workToken, int cacheToken, long timestamp, String key, long shardingKey)
      throws Exception {
    return buildInput(
        "work {"
            + "  computation_id: \""
            + DEFAULT_COMPUTATION_ID
            + "\""
            + "  input_data_watermark: 0"
            + "  work {"
            + "    key: \""
            + key
            + "\""
            + "    sharding_key: "
            + shardingKey
            + "    work_token: "
            + workToken
            + "    cache_token: "
            + cacheToken
            + "    hot_key_info {"
            + "      hot_key_age_usec: 1000000"
            + "    }"
            + "    message_bundles {"
            + "      source_computation_id: \""
            + DEFAULT_SOURCE_COMPUTATION_ID
            + "\""
            + "      messages {"
            + "        timestamp: "
            + timestamp
            + "        data: \"data"
            + workToken
            + "\""
            + "      }"
            + "    }"
            + "  }"
            + "}",
        CoderUtils.encodeToByteArray(
            CollectionCoder.of(IntervalWindow.getCoder()),
            Collections.singletonList(DEFAULT_WINDOW)));
  }

  /**
   * Returns a {@link
   * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest} builder parsed
   * from the provided text format proto.
   */
  private WorkItemCommitRequest.Builder parseCommitRequest(String output) throws Exception {
    WorkItemCommitRequest.Builder builder = Windmill.WorkItemCommitRequest.newBuilder();
    TextFormat.merge(output, builder);
    return builder;
  }

  /** Sets the metadata of all the contained messages in this WorkItemCommitRequest. */
  private WorkItemCommitRequest.Builder setMessagesMetadata(
      PaneInfo pane, byte[] windowBytes, WorkItemCommitRequest.Builder builder) throws Exception {
    if (windowBytes != null) {
      KeyedMessageBundle.Builder bundles = builder.getOutputMessagesBuilder(0).getBundlesBuilder(0);
      for (int i = 0; i < bundles.getMessagesCount(); i++) {
        bundles.getMessagesBuilder(i).setMetadata(addPaneTag(pane, windowBytes));
      }
    }
    return builder;
  }

  /** Reset value update timestamps to zero. */
  private WorkItemCommitRequest.Builder setValuesTimestamps(WorkItemCommitRequest.Builder builder) {
    for (int i = 0; i < builder.getValueUpdatesCount(); i++) {
      builder.getValueUpdatesBuilder(i).getValueBuilder().setTimestamp(0);
    }
    return builder;
  }

  private WorkItemCommitRequest.Builder makeExpectedOutput(int index, long timestamp)
      throws Exception {
    return makeExpectedOutput(
        index, timestamp, keyStringForIndex(index), DEFAULT_SHARDING_KEY, keyStringForIndex(index));
  }

  private WorkItemCommitRequest.Builder makeExpectedOutput(
      int index, long timestamp, String key, long shardingKey, String outKey) throws Exception {
    StringBuilder expectedCommitRequestBuilder =
        initializeExpectedCommitRequest(key, shardingKey, index);
    appendCommitOutputMessages(expectedCommitRequestBuilder, index, timestamp, outKey);

    return setMessagesMetadata(
        PaneInfo.NO_FIRING,
        intervalWindowBytes(DEFAULT_WINDOW),
        parseCommitRequest(expectedCommitRequestBuilder.toString()));
  }

  private WorkItemCommitRequest removeDynamicFields(WorkItemCommitRequest request) {
    // Throw away per_work_item_attribution because it is dynamic in tests.
    return request.toBuilder().clearPerWorkItemLatencyAttributions().build();
  }

  private WorkItemCommitRequest.Builder makeExpectedTruncationRequestOutput(
      int index, String key, long shardingKey, long estimatedSize) throws Exception {
    StringBuilder expectedCommitRequestBuilder =
        initializeExpectedCommitRequest(key, shardingKey, index, false);
    appendCommitTruncationFields(expectedCommitRequestBuilder, estimatedSize);

    return parseCommitRequest(expectedCommitRequestBuilder.toString());
  }

  private StringBuilder initializeExpectedCommitRequest(
      String key, long shardingKey, int index, Boolean hasSourceBytesProcessed) {
    StringBuilder requestBuilder = new StringBuilder();

    requestBuilder.append("key: \"");
    requestBuilder.append(key);
    requestBuilder.append("\" ");
    requestBuilder.append("sharding_key: ");
    requestBuilder.append(shardingKey);
    requestBuilder.append(" ");
    requestBuilder.append("work_token: ");
    requestBuilder.append(index);
    requestBuilder.append(" ");
    requestBuilder.append("cache_token: ");
    requestBuilder.append(index + 1);
    requestBuilder.append(" ");
    if (hasSourceBytesProcessed) {
      requestBuilder.append("source_bytes_processed: 0 ");
    }

    return requestBuilder;
  }

  private StringBuilder initializeExpectedCommitRequest(String key, long shardingKey, int index) {
    return initializeExpectedCommitRequest(key, shardingKey, index, true);
  }

  private StringBuilder appendCommitOutputMessages(
      StringBuilder requestBuilder, int index, long timestamp, String outKey) {
    requestBuilder.append("output_messages {");
    requestBuilder.append("  destination_stream_id: \"");
    requestBuilder.append(DEFAULT_DESTINATION_STREAM_ID);
    requestBuilder.append("\"");
    requestBuilder.append("  bundles {");
    requestBuilder.append("    key: \"");
    requestBuilder.append(outKey);
    requestBuilder.append("\"");
    requestBuilder.append("    messages {");
    requestBuilder.append("      timestamp: ");
    requestBuilder.append(timestamp);
    requestBuilder.append("      data: \"");
    requestBuilder.append(dataStringForIndex(index));
    requestBuilder.append("\"");
    requestBuilder.append("      metadata: \"\"");
    requestBuilder.append("    }");
    requestBuilder.append("    messages_ids: \"\"");
    requestBuilder.append("  }");
    requestBuilder.append("}");

    return requestBuilder;
  }

  private StringBuilder appendCommitTruncationFields(
      StringBuilder requestBuilder, long estimatedSize) {
    requestBuilder.append("exceeds_max_work_item_commit_bytes: true ");
    requestBuilder.append("estimated_work_item_commit_bytes: ");
    requestBuilder.append(estimatedSize);

    return requestBuilder;
  }

  private StreamingComputationConfig makeDefaultStreamingComputationConfig(
      List<ParallelInstruction> instructions) {
    StreamingComputationConfig config = new StreamingComputationConfig();
    config.setComputationId(DEFAULT_COMPUTATION_ID);
    config.setSystemName(DEFAULT_MAP_SYSTEM_NAME);
    config.setStageName(DEFAULT_MAP_STAGE_NAME);
    config.setInstructions(instructions);
    return config;
  }

  private ByteString addPaneTag(PaneInfo pane, byte[] windowBytes) throws IOException {
    ByteStringOutputStream output = new ByteStringOutputStream();
    PaneInfo.PaneInfoCoder.INSTANCE.encode(pane, output, Context.OUTER);
    output.write(windowBytes);
    return output.toByteString();
  }

  private DataflowWorkerHarnessOptions createTestingPipelineOptions(String... args) {
    List<String> argsList = Lists.newArrayList(args);
    if (streamingEngine) {
      argsList.add("--experiments=enable_streaming_engine");
    }
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(argsList.toArray(new String[0]))
            .as(DataflowWorkerHarnessOptions.class);
    options.setAppName("StreamingWorkerHarnessTest");
    options.setJobId("test_job_id");
    options.setProject("test_project");
    options.setWorkerId("test_worker");
    options.setStreaming(true);

    // If activeWorkRefreshPeriodMillis is the default value, override it.
    if (options.getActiveWorkRefreshPeriodMillis() == 10000) {
      options.setActiveWorkRefreshPeriodMillis(0);
    }

    return options;
  }

  private StreamingDataflowWorker makeWorker(
      StreamingDataflowWorkerTestParams streamingDataflowWorkerTestParams) {
    when(mockGlobalConfigHandle.getConfig())
        .thenReturn(streamingDataflowWorkerTestParams.streamingGlobalConfig());
    StreamingDataflowWorker worker =
        StreamingDataflowWorker.forTesting(
            streamingDataflowWorkerTestParams.stateNameMappings(),
            server,
            Collections.singletonList(
                defaultMapTask(streamingDataflowWorkerTestParams.instructions())),
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            mockWorkUnitClient,
            streamingDataflowWorkerTestParams.options(),
            streamingDataflowWorkerTestParams.publishCounters(),
            hotKeyLogger,
            streamingDataflowWorkerTestParams.clock(),
            streamingDataflowWorkerTestParams.executorSupplier(),
            mockGlobalConfigHandle,
            streamingDataflowWorkerTestParams.localRetryTimeoutMs(),
            streamingCounters,
            new FakeWindmillStubFactoryFactory(
                new FakeWindmillStubFactory(
                    () ->
                        WindmillChannelFactory.inProcessChannel(
                            "StreamingDataflowWorkerTestChannel"))));
    this.computationStateCache = worker.getComputationStateCache();
    return worker;
  }

  @Test
  public void testBasicHarness() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server.whenGetWorkCalled().thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)));
    }

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(numIters);
    worker.stop();

    for (int i = 0; i < numIters; ++i) {
      assertTrue(result.containsKey((long) i));
      assertEquals(
          makeExpectedOutput(i, TimeUnit.MILLISECONDS.toMicros(i)).build(),
          removeDynamicFields(result.get((long) i)));
    }

    verify(hotKeyLogger, atLeastOnce()).logHotKeyDetection(nullable(String.class), any());
  }

  private void runTestBasic(int numCommitThreads) throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingConfigTask streamingConfig = new StreamingConfigTask();
    streamingConfig.setStreamingComputationConfigs(
        ImmutableList.of(makeDefaultStreamingComputationConfig(instructions)));
    streamingConfig.setWindmillServiceEndpoint("foo");
    WorkItem workItem = new WorkItem();
    workItem.setStreamingConfigTask(streamingConfig);
    when(mockWorkUnitClient.getGlobalStreamingConfigWorkItem()).thenReturn(Optional.of(workItem));
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server.whenGetWorkCalled().thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)));
    }

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(numIters);
    worker.stop();

    for (int i = 0; i < numIters; ++i) {
      assertTrue(result.containsKey((long) i));
      assertEquals(
          makeExpectedOutput(i, TimeUnit.MILLISECONDS.toMicros(i)).build(),
          removeDynamicFields(result.get((long) i)));
    }

    verify(hotKeyLogger, atLeastOnce()).logHotKeyDetection(nullable(String.class), any());
  }

  @Test
  public void testBasic() throws Exception {
    runTestBasic(1);
  }

  @Test
  public void testBasicWithMultipleCommitThreads() throws Exception {
    runTestBasic(2);
  }

  @Test
  public void testHotKeyLogging() throws Exception {
    // This is to test that the worker can correctly log the key from a hot key.
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())),
            makeSinkInstruction(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), 0));

    StreamingConfigTask streamingConfig = new StreamingConfigTask();
    streamingConfig.setStreamingComputationConfigs(
        ImmutableList.of(makeDefaultStreamingComputationConfig(instructions)));
    streamingConfig.setWindmillServiceEndpoint("foo");
    WorkItem workItem = new WorkItem();
    workItem.setStreamingConfigTask(streamingConfig);
    when(mockWorkUnitClient.getGlobalStreamingConfigWorkItem()).thenReturn(Optional.of(workItem));
    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--hotKeyLoggingEnabled=true")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server
          .whenGetWorkCalled()
          .thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i), "key", DEFAULT_SHARDING_KEY));
    }

    server.waitForAndGetCommits(numIters);
    worker.stop();

    verify(hotKeyLogger, atLeastOnce())
        .logHotKeyDetection(nullable(String.class), any(), eq("key"));
  }

  @Test
  public void testHotKeyLoggingNotEnabled() throws Exception {
    // This is to test that the worker can correctly log the key from a hot key.
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())),
            makeSinkInstruction(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), 0));

    StreamingConfigTask streamingConfig = new StreamingConfigTask();
    streamingConfig.setStreamingComputationConfigs(
        ImmutableList.of(makeDefaultStreamingComputationConfig(instructions)));
    streamingConfig.setWindmillServiceEndpoint("foo");
    WorkItem workItem = new WorkItem();
    workItem.setStreamingConfigTask(streamingConfig);
    when(mockWorkUnitClient.getGlobalStreamingConfigWorkItem()).thenReturn(Optional.of(workItem));

    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    final int numIters = 2000;
    for (int i = 0; i < numIters; ++i) {
      server
          .whenGetWorkCalled()
          .thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i), "key", DEFAULT_SHARDING_KEY));
    }

    server.waitForAndGetCommits(numIters);
    worker.stop();

    verify(hotKeyLogger, atLeastOnce()).logHotKeyDetection(nullable(String.class), any());
  }

  @Test
  public void testIgnoreRetriedKeys() throws Exception {
    final int numIters = 4;
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(blockingFn, 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    for (int i = 0; i < numIters; ++i) {
      server
          .whenGetWorkCalled()
          .thenReturn(
              makeInput(
                  i, TimeUnit.MILLISECONDS.toMicros(i), keyStringForIndex(i), DEFAULT_SHARDING_KEY))
          // Also add work for a different shard of the same key.
          .thenReturn(
              makeInput(
                  i + 1000,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY + 1));
    }

    // Wait for keys to schedule.  They will be blocked.
    BlockingFn.counter.acquire(numIters * 2);

    // Re-add the work, it should be ignored due to the keys being active.
    for (int i = 0; i < numIters; ++i) {
      // Same work token.
      server
          .whenGetWorkCalled()
          .thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)))
          .thenReturn(
              makeInput(
                  i + 1000,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY + 1));
    }

    // Give all added calls a chance to run.
    server.waitForEmptyWorkQueue();

    for (int i = 0; i < numIters; ++i) {
      // Different work token same keys.
      server
          .whenGetWorkCalled()
          .thenReturn(
              makeInput(
                  i + numIters,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY));
    }

    // Give all added calls a chance to run.
    server.waitForEmptyWorkQueue();

    // Release the blocked calls.
    BlockingFn.blocker.countDown();

    // Verify the output
    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(numIters * 3);
    for (int i = 0; i < numIters; ++i) {
      assertTrue(result.containsKey((long) i));
      assertEquals(
          makeExpectedOutput(i, TimeUnit.MILLISECONDS.toMicros(i)).build(),
          removeDynamicFields(result.get((long) i)));
      assertTrue(result.containsKey((long) i + 1000));
      assertEquals(
          makeExpectedOutput(
                  i + 1000,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY + 1,
                  keyStringForIndex(i))
              .build(),
          removeDynamicFields(result.get((long) i + 1000)));
      assertTrue(result.containsKey((long) i + numIters));
      assertEquals(
          makeExpectedOutput(
                  i + numIters,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY,
                  keyStringForIndex(i))
              .build(),
          removeDynamicFields(result.get((long) i + numIters)));
    }

    // Re-add the work, it should process due to the keys no longer being active.
    for (int i = 0; i < numIters; ++i) {
      server
          .whenGetWorkCalled()
          .thenReturn(
              makeInput(
                  i + numIters * 2,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY));
    }
    result = server.waitForAndGetCommits(numIters);
    worker.stop();
    for (int i = 0; i < numIters; ++i) {
      assertTrue(result.containsKey((long) i + numIters * 2));
      assertEquals(
          makeExpectedOutput(
                  i + numIters * 2,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY,
                  keyStringForIndex(i))
              .build(),
          removeDynamicFields(result.get((long) i + numIters * 2)));
    }
  }

  @Test(timeout = 10000)
  public void testNumberOfWorkerHarnessThreadsIsHonored() throws Exception {
    int expectedNumberOfThreads = 5;
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(blockingFn, 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--numberOfWorkerHarnessThreads=" + expectedNumberOfThreads)
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    for (int i = 0; i < expectedNumberOfThreads * 2; ++i) {
      server.whenGetWorkCalled().thenReturn(makeInput(i, TimeUnit.MILLISECONDS.toMicros(i)));
    }

    // This will fail to complete if the number of threads is less than the amount of work.
    // Forcing this test to timeout.
    BlockingFn.counter.acquire(expectedNumberOfThreads);

    // Attempt to acquire an additional permit, if we were able to then that means
    // too many items were being processed concurrently.
    if (BlockingFn.counter.tryAcquire(500, TimeUnit.MILLISECONDS)) {
      fail(
          "Expected number of threads "
              + expectedNumberOfThreads
              + " does not match actual "
              + "number of work items processed concurrently "
              + BlockingFn.callCounter.get()
              + ".");
    }

    BlockingFn.blocker.countDown();
  }

  @Test
  public void testKeyTokenInvalidException() throws Exception {
    if (streamingEngine) {
      // TODO: This test needs to be adapted to work with streamingEngine=true.
      return;
    }
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(kvCoder),
            makeDoFnInstruction(new KeyTokenInvalidFn(), 0, kvCoder),
            makeSinkInstruction(kvCoder, 1));

    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(0, 0, DEFAULT_KEY_STRING, DEFAULT_SHARDING_KEY));

    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    server.waitForEmptyWorkQueue();

    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(1, 0, DEFAULT_KEY_STRING, DEFAULT_SHARDING_KEY));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    assertEquals(
        makeExpectedOutput(1, 0, DEFAULT_KEY_STRING, DEFAULT_SHARDING_KEY, DEFAULT_KEY_STRING)
            .build(),
        removeDynamicFields(result.get(1L)));
    assertEquals(1, result.size());
  }

  @Test
  public void testKeyCommitTooLargeException() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(kvCoder),
            makeDoFnInstruction(new LargeCommitFn(), 0, kvCoder),
            makeSinkInstruction(kvCoder, 1));

    server.setExpectedExceptionCount(1);

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(instructions)
                .setStreamingGlobalConfig(
                    StreamingGlobalConfig.builder()
                        .setOperationalLimits(
                            OperationalLimits.builder().setMaxWorkItemCommitBytes(1000).build())
                        .build())
                .publishCounters()
                .build());
    worker.start();

    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(1, 0, "large_key", DEFAULT_SHARDING_KEY))
        .thenReturn(makeInput(2, 0, "key", DEFAULT_SHARDING_KEY));
    server.waitForEmptyWorkQueue();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    assertEquals(2, result.size());
    assertEquals(
        makeExpectedOutput(2, 0, "key", DEFAULT_SHARDING_KEY, "key").build(),
        removeDynamicFields(result.get(2L)));

    assertTrue(result.containsKey(1L));
    WorkItemCommitRequest largeCommit = result.get(1L);
    assertEquals("large_key", largeCommit.getKey().toStringUtf8());
    assertEquals(
        makeExpectedTruncationRequestOutput(
                1, "large_key", DEFAULT_SHARDING_KEY, largeCommit.getEstimatedWorkItemCommitBytes())
            .build(),
        largeCommit);

    // Check this explicitly since the estimated commit bytes weren't actually
    // checked against an expected value in the previous step
    assertTrue(largeCommit.getEstimatedWorkItemCommitBytes() > 1000);

    // Spam worker updates a few times.
    int maxTries = 10;
    while (--maxTries > 0) {
      worker.reportPeriodicWorkerUpdatesForTest();
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }

    // We should see an exception reported for the large commit but not the small one.
    ArgumentCaptor<WorkItemStatus> workItemStatusCaptor =
        ArgumentCaptor.forClass(WorkItemStatus.class);
    verify(mockWorkUnitClient, atLeast(2)).reportWorkItemStatus(workItemStatusCaptor.capture());
    List<WorkItemStatus> capturedStatuses = workItemStatusCaptor.getAllValues();
    boolean foundErrors = false;
    for (WorkItemStatus status : capturedStatuses) {
      if (!status.getErrors().isEmpty()) {
        assertFalse(foundErrors);
        foundErrors = true;
        String errorMessage = status.getErrors().get(0).getMessage();
        assertThat(errorMessage, Matchers.containsString("KeyCommitTooLargeException"));
      }
    }
    assertTrue(foundErrors);
  }

  @Test
  public void testOutputKeyTooLargeException() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(kvCoder),
            makeDoFnInstruction(new ExceptionCatchingFn(), 0, kvCoder),
            makeSinkInstruction(kvCoder, 1));

    server.setExpectedExceptionCount(1);

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--experiments=throw_exceptions_on_large_output")
                .setInstructions(instructions)
                .setStreamingGlobalConfig(
                    StreamingGlobalConfig.builder()
                        .setOperationalLimits(
                            OperationalLimits.builder().setMaxOutputKeyBytes(15).build())
                        .build())
                .build());
    worker.start();

    // This large key will cause the ExceptionCatchingFn to throw an exception, which will then
    // cause it to output a smaller key.
    String bigKey = "some_much_too_large_output_key";
    server.whenGetWorkCalled().thenReturn(makeInput(1, 0, bigKey, DEFAULT_SHARDING_KEY));
    server.waitForEmptyWorkQueue();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    assertEquals(1, result.size());
    assertEquals(
        makeExpectedOutput(1, 0, bigKey, DEFAULT_SHARDING_KEY, "smaller_key").build(),
        removeDynamicFields(result.get(1L)));
  }

  @Test
  public void testOutputValueTooLargeException() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(kvCoder),
            makeDoFnInstruction(new ExceptionCatchingFn(), 0, kvCoder),
            makeSinkInstruction(kvCoder, 1));

    server.setExpectedExceptionCount(1);

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--experiments=throw_exceptions_on_large_output")
                .setInstructions(instructions)
                .setStreamingGlobalConfig(
                    StreamingGlobalConfig.builder()
                        .setOperationalLimits(
                            OperationalLimits.builder().setMaxOutputValueBytes(15).build())
                        .build())
                .build());
    worker.start();

    // The first time processing will have value "data1_a_bunch_more_data_output", which is above
    // the limit. After throwing the exception, the output should be just "data1", which is small
    // enough.
    server.whenGetWorkCalled().thenReturn(makeInput(1, 0, "key", DEFAULT_SHARDING_KEY));
    server.waitForEmptyWorkQueue();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    assertEquals(1, result.size());
    assertEquals(
        makeExpectedOutput(1, 0, "key", DEFAULT_SHARDING_KEY, "smaller_key").build(),
        removeDynamicFields(result.get(1L)));
  }

  @Test
  public void testKeyChange() throws Exception {
    KvCoder<String, String> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(kvCoder),
            makeDoFnInstruction(new ChangeKeysFn(), 0, kvCoder),
            makeSinkInstruction(kvCoder, 1));

    for (int i = 0; i < 2; i++) {
      server
          .whenGetWorkCalled()
          .thenReturn(
              makeInput(
                  i, TimeUnit.MILLISECONDS.toMicros(i), keyStringForIndex(i), DEFAULT_SHARDING_KEY))
          .thenReturn(
              makeInput(
                  i + 1000,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY + i));
    }

    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(4);

    for (int i = 0; i < 2; i++) {
      assertTrue(result.containsKey((long) i));
      assertEquals(
          makeExpectedOutput(
                  i,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY,
                  keyStringForIndex(i) + "_data" + i)
              .build(),
          removeDynamicFields(result.get((long) i)));
      assertTrue(result.containsKey((long) i + 1000));
      assertEquals(
          makeExpectedOutput(
                  i + 1000,
                  TimeUnit.MILLISECONDS.toMicros(i),
                  keyStringForIndex(i),
                  DEFAULT_SHARDING_KEY + i,
                  keyStringForIndex(i) + "_data" + (i + 1000))
              .build(),
          removeDynamicFields(result.get((long) i + 1000)));
    }
  }

  @Test(timeout = 30000)
  public void testExceptions() throws Exception {
    if (streamingEngine) {
      // TODO: This test needs to be adapted to work with streamingEngine=true.
      return;
    }

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new TestExceptionFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 1));

    server.setExpectedExceptionCount(1);
    String keyString = keyStringForIndex(0);
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \""
                    + DEFAULT_COMPUTATION_ID
                    + "\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \""
                    + keyString
                    + "\""
                    + "    sharding_key: 1"
                    + "    work_token: 0"
                    + "    cache_token: 1"
                    + "    message_bundles {"
                    + "      source_computation_id: \""
                    + DEFAULT_SOURCE_COMPUTATION_ID
                    + "\""
                    + "      messages {"
                    + "        timestamp: 0"
                    + "        data: \"0\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}",
                CoderUtils.encodeToByteArray(
                    CollectionCoder.of(IntervalWindow.getCoder()),
                    Collections.singletonList(DEFAULT_WINDOW))));

    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();
    server.waitForEmptyWorkQueue();

    // Wait until the worker has given up.
    int maxTries = 10;
    while (maxTries-- > 0 && !worker.workExecutorIsEmpty()) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
    assertTrue(worker.workExecutorIsEmpty());

    // Spam worker updates a few times.
    maxTries = 10;
    while (maxTries-- > 0) {
      worker.reportPeriodicWorkerUpdatesForTest();
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }

    // We should see our update only one time with the exceptions we are expecting.
    ArgumentCaptor<WorkItemStatus> workItemStatusCaptor =
        ArgumentCaptor.forClass(WorkItemStatus.class);
    verify(mockWorkUnitClient, atLeast(1)).reportWorkItemStatus(workItemStatusCaptor.capture());
    List<WorkItemStatus> capturedStatuses = workItemStatusCaptor.getAllValues();
    boolean foundErrors = false;
    int lastUpdateWithoutErrors = 0;
    int lastUpdateWithErrors = 0;
    for (WorkItemStatus status : capturedStatuses) {
      if (status.getErrors().isEmpty()) {
        lastUpdateWithoutErrors++;
        continue;
      }
      lastUpdateWithErrors++;
      assertFalse(foundErrors);
      foundErrors = true;
      String stacktrace = status.getErrors().get(0).getMessage();
      assertThat(stacktrace, Matchers.containsString("Exception!"));
      assertThat(stacktrace, Matchers.containsString("Another exception!"));
      assertThat(stacktrace, Matchers.containsString("processElement"));
    }
    assertTrue(foundErrors);

    // The last update we see should not have any errors. This indicates we've retried the workitem.
    assertTrue(lastUpdateWithoutErrors > lastUpdateWithErrors);

    // Confirm we've received the expected stats. There is no guarantee stats will only be reported
    // once.
    assertThat(server.getStatsReceived().size(), Matchers.greaterThanOrEqualTo(1));
    Windmill.ReportStatsRequest stats = server.getStatsReceived().get(0);
    assertEquals(DEFAULT_COMPUTATION_ID, stats.getComputationId());
    assertEquals(keyString, stats.getKey().toStringUtf8());
    assertEquals(0, stats.getWorkToken());
    assertEquals(1, stats.getShardingKey());
  }

  @Test
  public void testAssignWindows() throws Exception {
    Duration gapDuration = Duration.standardSeconds(1);
    CloudObject spec = CloudObject.forClassName("AssignWindowsDoFn");
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            WindowingStrategyTranslation.toMessageProto(
                    WindowingStrategy.of(FixedWindows.of(gapDuration)), sdkComponents)
                .toByteArray()));

    ParallelInstruction addWindowsInstruction =
        new ParallelInstruction()
            .setSystemName("AssignWindows")
            .setName("AssignWindows")
            .setOriginalName("AssignWindowsOriginal")
            .setParDo(
                new ParDoInstruction()
                    .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
                    .setNumOutputs(1)
                    .setUserFn(spec))
            .setOutputs(
                Collections.singletonList(
                    new InstructionOutput()
                        .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                        .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                        .setName("output")
                        .setCodec(
                            CloudObjects.asCloudObject(
                                WindowedValue.getFullCoder(
                                    StringUtf8Coder.of(), IntervalWindow.getCoder()),
                                /* sdkComponents= */ null))));

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            addWindowsInstruction,
            makeSinkInstruction(StringUtf8Coder.of(), 1));

    int timestamp1 = 0;
    int timestamp2 = 1000000;

    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(timestamp1, timestamp1))
        .thenReturn(makeInput(timestamp2, timestamp2));

    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).build());
    worker.start();

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(2);

    assertThat(
        removeDynamicFields(result.get((long) timestamp1)),
        equalTo(
            setMessagesMetadata(
                    PaneInfo.NO_FIRING,
                    intervalWindowBytes(WINDOW_AT_ZERO),
                    makeExpectedOutput(timestamp1, timestamp1))
                .build()));

    assertThat(
        removeDynamicFields(result.get((long) timestamp2)),
        equalTo(
            setMessagesMetadata(
                    PaneInfo.NO_FIRING,
                    intervalWindowBytes(WINDOW_AT_ONE_SECOND),
                    makeExpectedOutput(timestamp2, timestamp2))
                .build()));
  }

  private void verifyTimers(WorkItemCommitRequest commit, Timer... timers) {
    assertThat(commit.getOutputTimersList(), Matchers.containsInAnyOrder(timers));
  }

  private void verifyHolds(WorkItemCommitRequest commit, WatermarkHold... watermarkHolds) {
    assertThat(commit.getWatermarkHoldsList(), Matchers.containsInAnyOrder(watermarkHolds));
  }

  private Timer buildWatermarkTimer(String tagPrefix, long timestampMillis) {
    return buildWatermarkTimer(tagPrefix, timestampMillis, false);
  }

  private Timer buildWatermarkTimer(String tagPrefix, long timestampMillis, boolean delete) {
    Timer.Builder builder =
        Timer.newBuilder()
            .setTag(ByteString.copyFromUtf8(tagPrefix + ":" + timestampMillis))
            .setType(Type.WATERMARK)
            .setStateFamily("MergeWindows");
    if (!delete) {
      builder.setTimestamp(timestampMillis * 1000);
      builder.setMetadataTimestamp(timestampMillis * 1000);
    }
    return builder.build();
  }

  private WatermarkHold buildHold(String tag, long timestamp, boolean reset) {
    WatermarkHold.Builder builder =
        WatermarkHold.newBuilder()
            .setTag(ByteString.copyFromUtf8(tag))
            .setStateFamily("MergeWindows");
    if (reset) {
      builder.setReset(true);
    }
    if (timestamp >= 0) {
      builder.addTimestamps(timestamp * 1000);
    }
    return builder.build();
  }

  @Test
  // Runs a merging windows test verifying stored state, holds and timers.
  public void testMergeWindows() throws Exception {
    Coder<KV<String, String>> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    Coder<WindowedValue<KV<String, String>>> windowedKvCoder =
        FullWindowedValueCoder.of(kvCoder, IntervalWindow.getCoder());
    KvCoder<String, List<String>> groupedCoder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()));
    Coder<WindowedValue<KV<String, List<String>>>> windowedGroupedCoder =
        FullWindowedValueCoder.of(groupedCoder, IntervalWindow.getCoder());

    CloudObject spec = CloudObject.forClassName("MergeWindowsDoFn");
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            WindowingStrategyTranslation.toMessageProto(
                    WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(1)))
                        .withTimestampCombiner(TimestampCombiner.EARLIEST),
                    sdkComponents)
                .toByteArray()));
    addObject(
        spec,
        WorkerPropertyNames.INPUT_CODER,
        CloudObjects.asCloudObject(windowedKvCoder, /* sdkComponents= */ null));

    ParallelInstruction mergeWindowsInstruction =
        new ParallelInstruction()
            .setSystemName("MergeWindows-System")
            .setName("MergeWindowsStep")
            .setOriginalName("MergeWindowsOriginal")
            .setParDo(
                new ParDoInstruction()
                    .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
                    .setNumOutputs(1)
                    .setUserFn(spec))
            .setOutputs(
                Collections.singletonList(
                    new InstructionOutput()
                        .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                        .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                        .setName("output")
                        .setCodec(
                            CloudObjects.asCloudObject(
                                windowedGroupedCoder, /* sdkComponents= */ null))));

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeWindowingSourceInstruction(kvCoder),
            mergeWindowsInstruction,
            makeSinkInstruction(groupedCoder, 1));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(instructions)
                .addStateNameMapping("MergeWindowsStep", "MergeWindows")
                .build());
    worker.start();

    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \""
                    + DEFAULT_COMPUTATION_ID
                    + "\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \""
                    + DEFAULT_KEY_STRING
                    + "\""
                    + "    sharding_key: "
                    + DEFAULT_SHARDING_KEY
                    + "    cache_token: 1"
                    + "    work_token: 1"
                    + "    message_bundles {"
                    + "      source_computation_id: \""
                    + DEFAULT_SOURCE_COMPUTATION_ID
                    + "\""
                    + "      messages {"
                    + "        timestamp: 0"
                    + "        data: \""
                    + dataStringForIndex(0)
                    + "\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}",
                intervalWindowBytes(WINDOW_AT_ZERO)));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Iterable<CounterUpdate> counters = buildCounters();

    // These tags and data are opaque strings and this is a change detector test.
    // The "/u" indicates the user's namespace, versus "/s" for system namespace
    String window = "/gAAAAAAAA-joBw/";
    String timerTagPrefix = "/s" + window + "+0";
    ByteString bufferTag = ByteString.copyFromUtf8(window + "+ubuf");
    ByteString paneInfoTag = ByteString.copyFromUtf8(window + "+upane");
    String watermarkDataHoldTag = window + "+uhold";
    String watermarkExtraHoldTag = window + "+uextra";
    String stateFamily = "MergeWindows";
    ByteString bufferData = ByteString.copyFromUtf8("data0");
    // Encoded form for Iterable<String>: -1, true, 'data0', false
    ByteString outputData =
        ByteString.copyFrom(
            new byte[] {
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              0x01,
              0x05,
              0x64,
              0x61,
              0x74,
              0x61,
              0x30,
              0x00
            });

    // These values are not essential to the change detector test
    long timerTimestamp = 999000L;

    WorkItemCommitRequest actualOutput = result.get(1L);

    // Set timer
    verifyTimers(actualOutput, buildWatermarkTimer(timerTagPrefix, 999));

    assertThat(
        actualOutput.getBagUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagBag.newBuilder()
                    .setTag(bufferTag)
                    .setStateFamily(stateFamily)
                    .addValues(bufferData)
                    .build())));

    verifyHolds(actualOutput, buildHold(watermarkDataHoldTag, 0, false));

    // No state reads
    assertEquals(0L, splitIntToLong(getCounter(counters, "WindmillStateBytesRead").getInteger()));
    // Timer + buffer + watermark hold
    assertEquals(
        Windmill.WorkItemCommitRequest.newBuilder(actualOutput)
            .clearCounterUpdates()
            .clearOutputMessages()
            .clearPerWorkItemLatencyAttributions()
            .build()
            .getSerializedSize(),
        splitIntToLong(getCounter(counters, "WindmillStateBytesWritten").getInteger()));
    // Input messages
    assertEquals(
        VarInt.getLength(0L)
            + dataStringForIndex(0).length()
            + addPaneTag(PaneInfo.NO_FIRING, intervalWindowBytes(WINDOW_AT_ZERO)).size()
            + 5L // proto overhead
        ,
        splitIntToLong(getCounter(counters, "WindmillShuffleBytesRead").getInteger()));

    Windmill.GetWorkResponse.Builder getWorkResponse = Windmill.GetWorkResponse.newBuilder();
    getWorkResponse
        .addWorkBuilder()
        .setComputationId(DEFAULT_COMPUTATION_ID)
        .setInputDataWatermark(timerTimestamp + 1000)
        .addWorkBuilder()
        .setKey(ByteString.copyFromUtf8(DEFAULT_KEY_STRING))
        .setShardingKey(DEFAULT_SHARDING_KEY)
        .setWorkToken(2)
        .setCacheToken(1)
        .getTimersBuilder()
        .addTimers(buildWatermarkTimer(timerTagPrefix, timerTimestamp));
    server.whenGetWorkCalled().thenReturn(getWorkResponse.build());

    long expectedBytesRead = 0L;

    Windmill.GetDataResponse.Builder dataResponse = Windmill.GetDataResponse.newBuilder();
    Windmill.KeyedGetDataResponse.Builder dataBuilder =
        dataResponse
            .addDataBuilder()
            .setComputationId(DEFAULT_COMPUTATION_ID)
            .addDataBuilder()
            .setKey(ByteString.copyFromUtf8(DEFAULT_KEY_STRING))
            .setShardingKey(DEFAULT_SHARDING_KEY);
    dataBuilder
        .addBagsBuilder()
        .setTag(bufferTag)
        .setStateFamily(stateFamily)
        .addValues(bufferData);
    dataBuilder
        .addWatermarkHoldsBuilder()
        .setTag(ByteString.copyFromUtf8(watermarkDataHoldTag))
        .setStateFamily(stateFamily)
        .addTimestamps(0);
    dataBuilder
        .addWatermarkHoldsBuilder()
        .setTag(ByteString.copyFromUtf8(watermarkExtraHoldTag))
        .setStateFamily(stateFamily)
        .addTimestamps(0);
    dataBuilder
        .addValuesBuilder()
        .setTag(paneInfoTag)
        .setStateFamily(stateFamily)
        .getValueBuilder()
        .setTimestamp(0)
        .setData(ByteString.EMPTY);
    server.whenGetDataCalled().thenReturn(dataResponse.build());

    expectedBytesRead += dataBuilder.build().getSerializedSize();

    result = server.waitForAndGetCommits(1);
    counters = buildCounters();
    actualOutput = result.get(2L);

    assertEquals(1, actualOutput.getOutputMessagesCount());
    assertEquals(
        DEFAULT_DESTINATION_STREAM_ID, actualOutput.getOutputMessages(0).getDestinationStreamId());
    assertEquals(
        DEFAULT_KEY_STRING,
        actualOutput.getOutputMessages(0).getBundles(0).getKey().toStringUtf8());
    assertEquals(0, actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getTimestamp());
    assertEquals(
        outputData, actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getData());

    ByteString metadata =
        actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getMetadata();
    InputStream inStream = metadata.newInput();
    assertEquals(
        PaneInfo.createPane(true, true, Timing.ON_TIME), PaneInfoCoder.INSTANCE.decode(inStream));
    assertEquals(
        Collections.singletonList(WINDOW_AT_ZERO),
        DEFAULT_WINDOW_COLLECTION_CODER.decode(inStream, Coder.Context.OUTER));

    // Data was deleted
    assertThat(
        "" + actualOutput.getValueUpdatesList(),
        actualOutput.getValueUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagValue.newBuilder()
                    .setTag(paneInfoTag)
                    .setStateFamily(stateFamily)
                    .setValue(
                        Windmill.Value.newBuilder()
                            .setTimestamp(Long.MAX_VALUE)
                            .setData(ByteString.EMPTY))
                    .build())));

    assertThat(
        "" + actualOutput.getBagUpdatesList(),
        actualOutput.getBagUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagBag.newBuilder()
                    .setTag(bufferTag)
                    .setStateFamily(stateFamily)
                    .setDeleteAll(true)
                    .build())));

    verifyHolds(
        actualOutput,
        buildHold(watermarkDataHoldTag, -1, true),
        buildHold(watermarkExtraHoldTag, -1, true));

    // State reads for windowing
    assertEquals(
        expectedBytesRead,
        splitIntToLong(getCounter(counters, "WindmillStateBytesRead").getInteger()));
    // State updates to clear state
    assertEquals(
        Windmill.WorkItemCommitRequest.newBuilder(removeDynamicFields(actualOutput))
            .clearCounterUpdates()
            .clearOutputMessages()
            .build()
            .getSerializedSize(),
        splitIntToLong(getCounter(counters, "WindmillStateBytesWritten").getInteger()));
    // No input messages
    assertEquals(0L, splitIntToLong(getCounter(counters, "WindmillShuffleBytesRead").getInteger()));
  }

  @Test
  // Runs a merging windows test verifying stored state, holds and timers with caching due to
  // the first processing having is_new_key set.
  public void testMergeWindowsCaching() throws Exception {
    Coder<KV<String, String>> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    Coder<WindowedValue<KV<String, String>>> windowedKvCoder =
        FullWindowedValueCoder.of(kvCoder, IntervalWindow.getCoder());
    KvCoder<String, List<String>> groupedCoder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()));
    Coder<WindowedValue<KV<String, List<String>>>> windowedGroupedCoder =
        FullWindowedValueCoder.of(groupedCoder, IntervalWindow.getCoder());

    CloudObject spec = CloudObject.forClassName("MergeWindowsDoFn");
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            WindowingStrategyTranslation.toMessageProto(
                    WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(1)))
                        .withTimestampCombiner(TimestampCombiner.EARLIEST),
                    sdkComponents)
                .toByteArray()));
    addObject(
        spec,
        WorkerPropertyNames.INPUT_CODER,
        CloudObjects.asCloudObject(windowedKvCoder, /* sdkComponents= */ null));

    ParallelInstruction mergeWindowsInstruction =
        new ParallelInstruction()
            .setSystemName("MergeWindows-System")
            .setName("MergeWindowsStep")
            .setOriginalName("MergeWindowsOriginal")
            .setParDo(
                new ParDoInstruction()
                    .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
                    .setNumOutputs(1)
                    .setUserFn(spec))
            .setOutputs(
                Collections.singletonList(
                    new InstructionOutput()
                        .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                        .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                        .setName("output")
                        .setCodec(
                            CloudObjects.asCloudObject(
                                windowedGroupedCoder, /* sdkComponents= */ null))));

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeWindowingSourceInstruction(kvCoder),
            mergeWindowsInstruction,
            // Use multiple stages in the maptask to test caching with multiple stages.
            makeDoFnInstruction(new PassthroughDoFn(), 1, groupedCoder),
            makeSinkInstruction(groupedCoder, 2));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(instructions)
                .addStateNameMapping("MergeWindowsStep", "MergeWindows")
                .build());
    worker.start();

    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \""
                    + DEFAULT_COMPUTATION_ID
                    + "\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \""
                    + DEFAULT_KEY_STRING
                    + "\""
                    + "    sharding_key: "
                    + DEFAULT_SHARDING_KEY
                    + "    cache_token: 1"
                    + "    work_token: 1"
                    + "    is_new_key: 1"
                    + "    message_bundles {"
                    + "      source_computation_id: \""
                    + DEFAULT_SOURCE_COMPUTATION_ID
                    + "\""
                    + "      messages {"
                    + "        timestamp: 0"
                    + "        data: \""
                    + dataStringForIndex(0)
                    + "\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}",
                intervalWindowBytes(WINDOW_AT_ZERO)));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Iterable<CounterUpdate> counters = buildCounters();

    // These tags and data are opaque strings and this is a change detector test.
    // The "/u" indicates the user's namespace, versus "/s" for system namespace
    String window = "/gAAAAAAAA-joBw/";
    String timerTagPrefix = "/s" + window + "+0";
    ByteString bufferTag = ByteString.copyFromUtf8(window + "+ubuf");
    ByteString paneInfoTag = ByteString.copyFromUtf8(window + "+upane");
    String watermarkDataHoldTag = window + "+uhold";
    String watermarkExtraHoldTag = window + "+uextra";
    String stateFamily = "MergeWindows";
    ByteString bufferData = ByteString.copyFromUtf8("data0");
    // Encoded form for Iterable<String>: -1, true, 'data0', false
    ByteString outputData =
        ByteString.copyFrom(
            new byte[] {
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              0x01,
              0x05,
              0x64,
              0x61,
              0x74,
              0x61,
              0x30,
              0x00
            });

    // These values are not essential to the change detector test
    long timerTimestamp = 999000L;

    WorkItemCommitRequest actualOutput = result.get(1L);

    // Set timer
    verifyTimers(actualOutput, buildWatermarkTimer(timerTagPrefix, 999));

    assertThat(
        actualOutput.getBagUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagBag.newBuilder()
                    .setTag(bufferTag)
                    .setStateFamily(stateFamily)
                    .addValues(bufferData)
                    .build())));

    verifyHolds(actualOutput, buildHold(watermarkDataHoldTag, 0, false));

    // No state reads
    assertEquals(0L, splitIntToLong(getCounter(counters, "WindmillStateBytesRead").getInteger()));
    // Timer + buffer + watermark hold
    assertEquals(
        Windmill.WorkItemCommitRequest.newBuilder(removeDynamicFields(actualOutput))
            .clearCounterUpdates()
            .clearOutputMessages()
            .build()
            .getSerializedSize(),
        splitIntToLong(getCounter(counters, "WindmillStateBytesWritten").getInteger()));
    // Input messages
    assertEquals(
        VarInt.getLength(0L)
            + dataStringForIndex(0).length()
            + addPaneTag(PaneInfo.NO_FIRING, intervalWindowBytes(WINDOW_AT_ZERO)).size()
            + 5L // proto overhead
        ,
        splitIntToLong(getCounter(counters, "WindmillShuffleBytesRead").getInteger()));

    Windmill.GetWorkResponse.Builder getWorkResponse = Windmill.GetWorkResponse.newBuilder();
    getWorkResponse
        .addWorkBuilder()
        .setComputationId(DEFAULT_COMPUTATION_ID)
        .setInputDataWatermark(timerTimestamp + 1000)
        .addWorkBuilder()
        .setKey(ByteString.copyFromUtf8(DEFAULT_KEY_STRING))
        .setShardingKey(DEFAULT_SHARDING_KEY)
        .setWorkToken(2)
        .setCacheToken(1)
        .getTimersBuilder()
        .addTimers(buildWatermarkTimer(timerTagPrefix, timerTimestamp));
    server.whenGetWorkCalled().thenReturn(getWorkResponse.build());

    long expectedBytesRead = 0L;

    Windmill.GetDataResponse.Builder dataResponse = Windmill.GetDataResponse.newBuilder();
    Windmill.KeyedGetDataResponse.Builder dataBuilder =
        dataResponse
            .addDataBuilder()
            .setComputationId(DEFAULT_COMPUTATION_ID)
            .addDataBuilder()
            .setKey(ByteString.copyFromUtf8(DEFAULT_KEY_STRING))
            .setShardingKey(DEFAULT_SHARDING_KEY);
    // These reads are skipped due to being cached from accesses in the first work item processing.
    // dataBuilder
    //     .addBagsBuilder()
    //     .setTag(bufferTag)
    //     .setStateFamily(stateFamily)
    //     .addValues(bufferData);
    // dataBuilder
    //     .addWatermarkHoldsBuilder()
    //     .setTag(ByteString.copyFromUtf8(watermarkDataHoldTag))
    //     .setStateFamily(stateFamily)
    //     .addTimestamps(0);
    dataBuilder
        .addWatermarkHoldsBuilder()
        .setTag(ByteString.copyFromUtf8(watermarkExtraHoldTag))
        .setStateFamily(stateFamily)
        .addTimestamps(0);
    dataBuilder
        .addValuesBuilder()
        .setTag(paneInfoTag)
        .setStateFamily(stateFamily)
        .getValueBuilder()
        .setTimestamp(0)
        .setData(ByteString.EMPTY);
    server.whenGetDataCalled().thenReturn(dataResponse.build());

    expectedBytesRead += dataBuilder.build().getSerializedSize();

    result = server.waitForAndGetCommits(1);
    counters = buildCounters();
    actualOutput = result.get(2L);

    assertEquals(1, actualOutput.getOutputMessagesCount());
    assertEquals(
        DEFAULT_DESTINATION_STREAM_ID, actualOutput.getOutputMessages(0).getDestinationStreamId());
    assertEquals(
        DEFAULT_KEY_STRING,
        actualOutput.getOutputMessages(0).getBundles(0).getKey().toStringUtf8());
    assertEquals(0, actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getTimestamp());
    assertEquals(
        outputData, actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getData());

    ByteString metadata =
        actualOutput.getOutputMessages(0).getBundles(0).getMessages(0).getMetadata();
    InputStream inStream = metadata.newInput();
    assertEquals(
        PaneInfo.createPane(true, true, Timing.ON_TIME), PaneInfoCoder.INSTANCE.decode(inStream));
    assertEquals(
        Collections.singletonList(WINDOW_AT_ZERO),
        DEFAULT_WINDOW_COLLECTION_CODER.decode(inStream, Coder.Context.OUTER));

    // Data was deleted
    assertThat(
        "" + actualOutput.getValueUpdatesList(),
        actualOutput.getValueUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagValue.newBuilder()
                    .setTag(paneInfoTag)
                    .setStateFamily(stateFamily)
                    .setValue(
                        Windmill.Value.newBuilder()
                            .setTimestamp(Long.MAX_VALUE)
                            .setData(ByteString.EMPTY))
                    .build())));

    assertThat(
        "" + actualOutput.getBagUpdatesList(),
        actualOutput.getBagUpdatesList(),
        Matchers.contains(
            Matchers.equalTo(
                Windmill.TagBag.newBuilder()
                    .setTag(bufferTag)
                    .setStateFamily(stateFamily)
                    .setDeleteAll(true)
                    .build())));

    verifyHolds(
        actualOutput,
        buildHold(watermarkDataHoldTag, -1, true),
        buildHold(watermarkExtraHoldTag, -1, true));

    // State reads for windowing
    assertEquals(
        expectedBytesRead,
        splitIntToLong(getCounter(counters, "WindmillStateBytesRead").getInteger()));
    // State updates to clear state
    assertEquals(
        Windmill.WorkItemCommitRequest.newBuilder(removeDynamicFields(actualOutput))
            .clearCounterUpdates()
            .clearOutputMessages()
            .build()
            .getSerializedSize(),
        splitIntToLong(getCounter(counters, "WindmillStateBytesWritten").getInteger()));
    // No input messages
    assertEquals(0L, splitIntToLong(getCounter(counters, "WindmillShuffleBytesRead").getInteger()));

    CacheStats stats = worker.getStateCacheStats();
    LOG.info("cache stats {}", stats);
    assertEquals(1, stats.hitCount());
    assertEquals(4, stats.missCount());
  }

  // Helper for running tests for merging sessions based upon Actions consisting of GetWorkResponse
  // and expected timers and holds in the corresponding commit. All GetData requests are responded
  // to with empty state, relying on user worker caching to keep data written.
  private void runMergeSessionsActions(List<Action> actions) throws Exception {
    Coder<KV<String, String>> kvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    Coder<WindowedValue<KV<String, String>>> windowedKvCoder =
        FullWindowedValueCoder.of(kvCoder, IntervalWindow.getCoder());
    KvCoder<String, List<String>> groupedCoder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()));
    Coder<WindowedValue<KV<String, List<String>>>> windowedGroupedCoder =
        FullWindowedValueCoder.of(groupedCoder, IntervalWindow.getCoder());

    CloudObject spec = CloudObject.forClassName("MergeWindowsDoFn");
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(
            WindowingStrategyTranslation.toMessageProto(
                    WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
                        .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
                        .withTrigger(
                            Repeatedly.forever(
                                AfterWatermark.pastEndOfWindow()
                                    .withLateFirings(AfterPane.elementCountAtLeast(1))))
                        .withAllowedLateness(Duration.standardMinutes(60)),
                    sdkComponents)
                .toByteArray()));
    addObject(
        spec,
        WorkerPropertyNames.INPUT_CODER,
        CloudObjects.asCloudObject(windowedKvCoder, /* sdkComponents= */ null));

    ParallelInstruction mergeWindowsInstruction =
        new ParallelInstruction()
            .setSystemName("MergeWindows-System")
            .setName("MergeWindowsStep")
            .setOriginalName("MergeWindowsOriginal")
            .setParDo(
                new ParDoInstruction()
                    .setInput(new InstructionInput().setProducerInstructionIndex(0).setOutputNum(0))
                    .setNumOutputs(1)
                    .setUserFn(spec))
            .setOutputs(
                Collections.singletonList(
                    new InstructionOutput()
                        .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                        .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                        .setName("output")
                        .setCodec(
                            CloudObjects.asCloudObject(
                                windowedGroupedCoder, /* sdkComponents= */ null))));

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeWindowingSourceInstruction(kvCoder),
            mergeWindowsInstruction,
            makeSinkInstruction(groupedCoder, 1));
    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(instructions)
                .addStateNameMapping("MergeWindowsStep", "MergeWindows")
                .build());
    worker.start();

    // Respond to any GetData requests with empty state.
    server.whenGetDataCalled().answerByDefault(EMPTY_DATA_RESPONDER);

    for (int i = 0; i < actions.size(); ++i) {
      Action action = actions.get(i);
      server.whenGetWorkCalled().thenReturn(action.response);
      Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
      WorkItemCommitRequest actualOutput = result.get(i + 1L);
      assertThat(actualOutput, Matchers.not(Matchers.nullValue()));
      verifyTimers(actualOutput, action.expectedTimers);
      verifyHolds(actualOutput, action.expectedHolds);
    }
  }

  @Test
  public void testMergeSessionWindows_singleLateWindow() throws Exception {
    runMergeSessionsActions(
        Collections.singletonList(
            new Action(
                    buildSessionInput(
                        1, 40, 0, Collections.singletonList(1L), Collections.EMPTY_LIST))
                .withHolds(
                    buildHold("/gAAAAAAAAAsK/+uhold", -1, true),
                    buildHold("/gAAAAAAAAAsK/+uextra", -1, true))
                .withTimers(buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 3600010))));
  }

  @Test
  public void testMergeSessionWindows() throws Exception {
    // Test the behavior with an:
    // - on time window that is triggered due to watermark advancement
    // - a late window that is triggered immediately due to count
    // - extending the on time window to a timestamp beyond the watermark
    // - merging the late window with the on time window through the addition of intermediate
    // elements
    runMergeSessionsActions(
        Arrays.asList(
            new Action(
                    buildSessionInput(
                        1, 0, 0, Collections.singletonList(1L), Collections.EMPTY_LIST))
                .withHolds(buildHold("/gAAAAAAAAAsK/+uhold", 10, false))
                .withTimers(
                    buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 10),
                    buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 3600010)),
            new Action(
                    buildSessionInput(
                        2,
                        30,
                        0,
                        Collections.EMPTY_LIST,
                        Collections.singletonList(buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 10))))
                .withTimers(buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 3600010))
                .withHolds(
                    buildHold("/gAAAAAAAAAsK/+uhold", -1, true),
                    buildHold("/gAAAAAAAAAsK/+uextra", -1, true)),
            new Action(
                    buildSessionInput(
                        3, 30, 0, Collections.singletonList(8L), Collections.EMPTY_LIST))
                .withTimers(
                    buildWatermarkTimer("/s/gAAAAAAAABIR/+0", 3600017),
                    buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 10, true),
                    buildWatermarkTimer("/s/gAAAAAAAAAsK/+0", 3600010, true))
                .withHolds(
                    buildHold("/gAAAAAAAAAsK/+uhold", -1, true),
                    buildHold("/gAAAAAAAAAsK/+uextra", -1, true)),
            new Action(
                    buildSessionInput(
                        4, 30, 0, Collections.singletonList(31L), Collections.EMPTY_LIST))
                .withTimers(
                    buildWatermarkTimer("/s/gAAAAAAAACkK/+0", 3600040),
                    buildWatermarkTimer("/s/gAAAAAAAACkK/+0", 40))
                .withHolds(buildHold("/gAAAAAAAACkK/+uhold", 40, false)),
            new Action(buildSessionInput(5, 30, 0, Arrays.asList(17L, 23L), Collections.EMPTY_LIST))
                .withTimers(
                    buildWatermarkTimer("/s/gAAAAAAAACkK/+0", 3600040, true),
                    buildWatermarkTimer("/s/gAAAAAAAACkK/+0", 40, true),
                    buildWatermarkTimer("/s/gAAAAAAAABIR/+0", 3600017, true),
                    buildWatermarkTimer("/s/gAAAAAAAABIR/+0", 17, true),
                    buildWatermarkTimer("/s/gAAAAAAAACko/+0", 40),
                    buildWatermarkTimer("/s/gAAAAAAAACko/+0", 3600040))
                .withHolds(
                    buildHold("/gAAAAAAAACkK/+uhold", -1, true),
                    buildHold("/gAAAAAAAACkK/+uextra", -1, true),
                    buildHold("/gAAAAAAAAAsK/+uhold", 40, true),
                    buildHold("/gAAAAAAAAAsK/+uextra", 3600040, true)),
            new Action(
                    buildSessionInput(
                        6,
                        50,
                        0,
                        Collections.EMPTY_LIST,
                        Collections.singletonList(buildWatermarkTimer("/s/gAAAAAAAACko/+0", 40))))
                .withTimers(buildWatermarkTimer("/s/gAAAAAAAACko/+0", 3600040))
                .withHolds(
                    buildHold("/gAAAAAAAAAsK/+uhold", -1, true),
                    buildHold("/gAAAAAAAAAsK/+uextra", -1, true))));
  }

  private List<ParallelInstruction> makeUnboundedSourcePipeline() throws Exception {
    return makeUnboundedSourcePipeline(1, new PrintFn());
  }

  private List<ParallelInstruction> makeUnboundedSourcePipeline(
      int numMessagesPerShard, // Total number of messages in each split of the unbounded source.
      DoFn<ValueWithRecordId<KV<Integer, Integer>>, String> doFn)
      throws Exception {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setNumWorkers(1);
    CloudObject codec =
        CloudObjects.asCloudObject(
            WindowedValue.getFullCoder(
                ValueWithRecordId.ValueWithRecordIdCoder.of(
                    KvCoder.of(VarIntCoder.of(), VarIntCoder.of())),
                GlobalWindow.Coder.INSTANCE),
            /* sdkComponents= */ null);

    return Arrays.asList(
        new ParallelInstruction()
            .setSystemName("Read")
            .setOriginalName("OriginalReadName")
            .setRead(
                new ReadInstruction()
                    .setSource(
                        CustomSources.serializeToCloudSource(
                                new TestCountingSource(numMessagesPerShard), options)
                            .setCodec(codec)))
            .setOutputs(
                Collections.singletonList(
                    new InstructionOutput()
                        .setName("read_output")
                        .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                        .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                        .setCodec(codec))),
        makeDoFnInstruction(doFn, 0, StringUtf8Coder.of(), WindowingStrategy.globalDefault()),
        makeSinkInstruction(StringUtf8Coder.of(), 1, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testUnboundedSources() throws Exception {
    List<Integer> finalizeTracker = Lists.newArrayList();
    TestCountingSource.setFinalizeTracker(finalizeTracker);
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(makeUnboundedSourcePipeline()).build());
    worker.start();

    // Test new key.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 1"
                    + "    cache_token: 1"
                    + "  }"
                    + "}",
                null));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Iterable<CounterUpdate> counters = buildCounters();
    Windmill.WorkItemCommitRequest commit = result.get(1L);
    UnsignedLong finalizeId =
        UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(
        removeDynamicFields(commit),
        equalTo(
            setMessagesMetadata(
                    PaneInfo.NO_FIRING,
                    CoderUtils.encodeToByteArray(
                        CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                        Collections.singletonList(GlobalWindow.INSTANCE)),
                    parseCommitRequest(
                        "key: \"0000000000000001\" "
                            + "sharding_key: 1 "
                            + "work_token: 1 "
                            + "cache_token: 1 "
                            + "source_backlog_bytes: 7 "
                            + "source_bytes_processed: 18 "
                            + "output_messages {"
                            + "  destination_stream_id: \"out\""
                            + "  bundles {"
                            + "    key: \"0000000000000001\""
                            + "    messages {"
                            + "      timestamp: 0"
                            + "      data: \"0:0\""
                            + "    }"
                            + "    messages_ids: \"\""
                            + "  }"
                            + "} "
                            + "source_state_updates {"
                            + "  state: \"\000\""
                            + "  finalize_ids: "
                            + finalizeId
                            + "} "
                            + "source_watermark: 1000"))
                .build()));

    // Test same key continuing. The counter is done.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 2"
                    + "    cache_token: 1"
                    + "    source_state {"
                    + "      state: \"\001\""
                    + "      finalize_ids: "
                    + finalizeId
                    + "    } "
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);
    counters = buildCounters();

    commit = result.get(2L);
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(
        removeDynamicFields(commit),
        equalTo(
            parseCommitRequest(
                    "key: \"0000000000000001\" "
                        + "sharding_key: 1 "
                        + "work_token: 2 "
                        + "cache_token: 1 "
                        + "source_backlog_bytes: 7 "
                        + "source_bytes_processed: 0 "
                        + "source_state_updates {"
                        + "  state: \"\000\""
                        + "  finalize_ids: "
                        + finalizeId
                        + "} "
                        + "source_watermark: 1000")
                .build()));

    assertThat(finalizeTracker, contains(0));

    assertNull(getCounter(counters, "dataflow_input_size-computation"));

    // Test recovery (on a new key so fresh reader state). Counter is done.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000002\""
                    + "    sharding_key: 2"
                    + "    work_token: 3"
                    + "    cache_token: 2"
                    + "    source_state {"
                    + "      state: \"\000\""
                    + "    } "
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);
    counters = buildCounters();

    commit = result.get(3L);
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(
        removeDynamicFields(commit),
        equalTo(
            parseCommitRequest(
                    "key: \"0000000000000002\" "
                        + "sharding_key: 2 "
                        + "work_token: 3 "
                        + "cache_token: 2 "
                        + "source_backlog_bytes: 7 "
                        + "source_bytes_processed: 0 "
                        + "source_state_updates {"
                        + "  state: \"\000\""
                        + "  finalize_ids: "
                        + finalizeId
                        + "} "
                        + "source_watermark: 1000")
                .build()));

    assertNull(getCounter(counters, "dataflow_input_size-computation"));
  }

  @Test
  public void testUnboundedSourcesDrain() throws Exception {
    List<Integer> finalizeTracker = Lists.newArrayList();
    TestCountingSource.setFinalizeTracker(finalizeTracker);

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(makeUnboundedSourcePipeline())
                .publishCounters()
                .build());
    worker.start();

    // Test new key.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 2"
                    + "    cache_token: 3"
                    + "  }"
                    + "}",
                null));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    Windmill.WorkItemCommitRequest commit = result.get(2L);
    UnsignedLong finalizeId =
        UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(
        removeDynamicFields(commit),
        equalTo(
            setMessagesMetadata(
                    PaneInfo.NO_FIRING,
                    CoderUtils.encodeToByteArray(
                        CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                        Collections.singletonList(GlobalWindow.INSTANCE)),
                    parseCommitRequest(
                        "key: \"0000000000000001\" "
                            + "sharding_key: 1 "
                            + "work_token: 2 "
                            + "cache_token: 3 "
                            + "source_backlog_bytes: 7 "
                            + "source_bytes_processed: 18 "
                            + "output_messages {"
                            + "  destination_stream_id: \"out\""
                            + "  bundles {"
                            + "    key: \"0000000000000001\""
                            + "    messages {"
                            + "      timestamp: 0"
                            + "      data: \"0:0\""
                            + "    }"
                            + "    messages_ids: \"\""
                            + "  }"
                            + "} "
                            + "source_state_updates {"
                            + "  state: \"\000\""
                            + "  finalize_ids: "
                            + finalizeId
                            + "} "
                            + "source_watermark: 1000"))
                .build()));

    // Test drain work item.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 3"
                    + "    cache_token: 3"
                    + "    source_state {"
                    + "      only_finalize: true"
                    + "      finalize_ids: "
                    + finalizeId
                    + "    }"
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);

    commit = result.get(3L);

    assertThat(
        commit,
        equalTo(
            parseCommitRequest(
                    "key: \"0000000000000001\" "
                        + "sharding_key: 1 "
                        + "work_token: 3 "
                        + "cache_token: 3 "
                        + "source_state_updates {"
                        + "  only_finalize: true"
                        + "} ")
                .build()));

    assertThat(finalizeTracker, contains(0));
  }

  // Regression test to ensure that a reader is not used from the cache
  // on work item retry.
  @Test
  public void testUnboundedSourceWorkRetry() throws Exception {
    List<Integer> finalizeTracker = Lists.newArrayList();
    TestCountingSource.setFinalizeTracker(finalizeTracker);

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams(
                    "--workerCacheMb=0") // Disable state cache so it doesn't detect retry.
                .setInstructions(makeUnboundedSourcePipeline())
                .build());
    worker.start();

    // Test new key.
    Windmill.GetWorkResponse work =
        buildInput(
            "work {"
                + "  computation_id: \"computation\""
                + "  input_data_watermark: 0"
                + "  work {"
                + "    key: \"0000000000000001\""
                + "    sharding_key: 1"
                + "    work_token: 1"
                + "    cache_token: 1"
                + "  }"
                + "}",
            null);
    server.whenGetWorkCalled().thenReturn(work);

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Iterable<CounterUpdate> counters = buildCounters();
    Windmill.WorkItemCommitRequest commit = result.get(1L);
    UnsignedLong finalizeId =
        UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    Windmill.WorkItemCommitRequest expectedCommit =
        setMessagesMetadata(
                PaneInfo.NO_FIRING,
                CoderUtils.encodeToByteArray(
                    CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                    Collections.singletonList(GlobalWindow.INSTANCE)),
                parseCommitRequest(
                    "key: \"0000000000000001\" "
                        + "sharding_key: 1 "
                        + "work_token: 1 "
                        + "cache_token: 1 "
                        + "source_backlog_bytes: 7 "
                        + "source_bytes_processed: 18 "
                        + "output_messages {"
                        + "  destination_stream_id: \"out\""
                        + "  bundles {"
                        + "    key: \"0000000000000001\""
                        + "    messages {"
                        + "      timestamp: 0"
                        + "      data: \"0:0\""
                        + "    }"
                        + "    messages_ids: \"\""
                        + "  }"
                        + "} "
                        + "source_state_updates {"
                        + "  state: \"\000\""
                        + "  finalize_ids: "
                        + finalizeId
                        + "} "
                        + "source_watermark: 1000"))
            .build();

    assertThat(removeDynamicFields(commit), equalTo(expectedCommit));

    // Test retry of work item, it should return the same result and not start the reader from the
    // position it was left at.
    server.clearCommitsReceived();
    server.whenGetWorkCalled().thenReturn(work);
    result = server.waitForAndGetCommits(1);
    commit = result.get(1L);
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));
    Windmill.WorkItemCommitRequest.Builder commitBuilder = expectedCommit.toBuilder();
    commitBuilder
        .getSourceStateUpdatesBuilder()
        .setFinalizeIds(0, commit.getSourceStateUpdates().getFinalizeIds(0));
    expectedCommit = commitBuilder.build();
    assertThat(removeDynamicFields(commit), equalTo(expectedCommit));

    // Continue with processing.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 2"
                    + "    cache_token: 1"
                    + "    source_state {"
                    + "      state: \"\001\""
                    + "      finalize_ids: "
                    + finalizeId
                    + "    } "
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);

    commit = result.get(2L);
    finalizeId = UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

    assertThat(
        removeDynamicFields(commit),
        equalTo(
            parseCommitRequest(
                    "key: \"0000000000000001\" "
                        + "sharding_key: 1 "
                        + "work_token: 2 "
                        + "cache_token: 1 "
                        + "source_backlog_bytes: 7 "
                        + "source_bytes_processed: 0 "
                        + "source_state_updates {"
                        + "  state: \"\000\""
                        + "  finalize_ids: "
                        + finalizeId
                        + "} "
                        + "source_watermark: 1000")
                .build()));

    assertThat(finalizeTracker, contains(0));
  }

  @Test
  public void testActiveWork() {
    BoundedQueueExecutor mockExecutor = Mockito.mock(BoundedQueueExecutor.class);
    String computationId = "computation";
    ComputationState computationState =
        new ComputationState(
            computationId,
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            mockExecutor,
            ImmutableMap.of(),
            null);

    ShardedKey key1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);

    ExecutableWork m1 = createMockWork(key1, 1, computationId);
    assertTrue(computationState.activateWork(m1));
    Mockito.verify(mockExecutor).execute(m1, m1.getWorkItem().getSerializedSize());
    computationState.completeWorkAndScheduleNextWorkForKey(key1, m1.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);

    // Verify work queues.
    ExecutableWork m2 = createMockWork(key1, 2, computationId);
    assertTrue(computationState.activateWork(m2));
    Mockito.verify(mockExecutor).execute(m2, m2.getWorkItem().getSerializedSize());
    ExecutableWork m3 = createMockWork(key1, 3, computationId);
    assertTrue(computationState.activateWork(m3));
    Mockito.verifyNoMoreInteractions(mockExecutor);

    // Verify another key is a separate queue.
    ShardedKey key2 = ShardedKey.create(ByteString.copyFromUtf8("key2"), 2);
    ExecutableWork m4 = createMockWork(key2, 4, computationId);
    assertTrue(computationState.activateWork(m4));
    Mockito.verify(mockExecutor).execute(m4, m4.getWorkItem().getSerializedSize());
    computationState.completeWorkAndScheduleNextWorkForKey(key2, m4.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);

    computationState.completeWorkAndScheduleNextWorkForKey(key1, m2.id());
    Mockito.verify(mockExecutor).forceExecute(m3, m3.getWorkItem().getSerializedSize());
    computationState.completeWorkAndScheduleNextWorkForKey(key1, m3.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);

    // Verify duplicate work dropped.
    ExecutableWork m5 = createMockWork(key1, 5, computationId);
    computationState.activateWork(m5);
    Mockito.verify(mockExecutor).execute(m5, m5.getWorkItem().getSerializedSize());
    assertFalse(computationState.activateWork(m5));
    Mockito.verifyNoMoreInteractions(mockExecutor);
    computationState.completeWorkAndScheduleNextWorkForKey(key1, m5.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);
  }

  @Test
  public void testActiveWorkForShardedKeys() {
    BoundedQueueExecutor mockExecutor = Mockito.mock(BoundedQueueExecutor.class);
    String computationId = "computation";
    ComputationState computationState =
        new ComputationState(
            computationId,
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            mockExecutor,
            ImmutableMap.of(),
            null);

    ShardedKey key1Shard1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);
    ShardedKey key1Shard2 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 2);

    ExecutableWork m1 = createMockWork(key1Shard1, 1, computationId);
    assertTrue(computationState.activateWork(m1));
    Mockito.verify(mockExecutor).execute(m1, m1.getWorkItem().getSerializedSize());
    computationState.completeWorkAndScheduleNextWorkForKey(key1Shard1, m1.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);

    // Verify work queues.
    ExecutableWork m2 = createMockWork(key1Shard1, 2, computationId);
    assertTrue(computationState.activateWork(m2));
    Mockito.verify(mockExecutor).execute(m2, m2.getWorkItem().getSerializedSize());
    ExecutableWork m3 = createMockWork(key1Shard1, 3, computationId);
    assertTrue(computationState.activateWork(m3));
    Mockito.verifyNoMoreInteractions(mockExecutor);

    // Verify a different shard of key is a separate queue.
    ExecutableWork m4 = createMockWork(key1Shard1, 3, computationId);
    assertFalse(computationState.activateWork(m4));
    Mockito.verifyNoMoreInteractions(mockExecutor);
    ExecutableWork m4Shard2 = createMockWork(key1Shard2, 3, computationId);
    assertTrue(computationState.activateWork(m4Shard2));
    Mockito.verify(mockExecutor).execute(m4Shard2, m4Shard2.getWorkItem().getSerializedSize());

    // Verify duplicate work dropped
    assertFalse(computationState.activateWork(m4Shard2));
    computationState.completeWorkAndScheduleNextWorkForKey(key1Shard2, m4.id());
    Mockito.verifyNoMoreInteractions(mockExecutor);
  }

  @Test
  @Ignore // Test is flaky on Jenkins (#27555)
  public void testMaxThreadMetric() throws Exception {
    int maxThreads = 2;
    int threadExpiration = 60;
    // setting up actual implementation of executor instead of mocking to keep track of
    // active thread count.
    BoundedQueueExecutor executor =
        new BoundedQueueExecutor(
            maxThreads,
            threadExpiration,
            TimeUnit.SECONDS,
            maxThreads,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());

    ComputationState computationState =
        new ComputationState(
            "computation",
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            executor,
            ImmutableMap.of(),
            null);

    ShardedKey key1Shard1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);

    // overriding definition of MockWork to add sleep, which will help us keep track of how
    // long each work item takes to process and therefore let us manipulate how long the time
    // at which we're at max threads is.
    Consumer<Work> sleepProcessWorkFn =
        unused -> {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };

    ExecutableWork m2 = createMockWork(key1Shard1, 2, sleepProcessWorkFn);
    ExecutableWork m3 = createMockWork(key1Shard1, 3, sleepProcessWorkFn);

    assertTrue(computationState.activateWork(m2));
    assertTrue(computationState.activateWork(m3));
    executor.execute(m2, m2.getWorkItem().getSerializedSize());

    executor.execute(m3, m3.getWorkItem().getSerializedSize());

    // Will get close to 1000ms that both work items are processing (sleeping, really)
    // give or take a few ms.
    long i = 990L;
    assertTrue(executor.allThreadsActiveTime() >= i);
    executor.shutdown();
  }

  @Test
  public void testActiveThreadMetric() throws Exception {
    int maxThreads = 5;
    int threadExpirationSec = 60;
    CountDownLatch processStart1 = new CountDownLatch(2);
    CountDownLatch processStart2 = new CountDownLatch(3);
    CountDownLatch processStart3 = new CountDownLatch(4);
    AtomicBoolean stop = new AtomicBoolean(false);
    // setting up actual implementation of executor instead of mocking to keep track of
    // active thread count.
    BoundedQueueExecutor executor =
        new BoundedQueueExecutor(
            maxThreads,
            threadExpirationSec,
            TimeUnit.SECONDS,
            maxThreads,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());

    ComputationState computationState =
        new ComputationState(
            "computation",
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            executor,
            ImmutableMap.of(),
            null);

    ShardedKey key1Shard1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);

    Consumer<Work> sleepProcessWorkFn =
        unused -> {
          processStart1.countDown();
          processStart2.countDown();
          processStart3.countDown();
          int count = 0;
          while (!stop.get()) {
            count += 1;
          }
        };

    ExecutableWork m2 = createMockWork(key1Shard1, 2, sleepProcessWorkFn);

    ExecutableWork m3 = createMockWork(key1Shard1, 3, sleepProcessWorkFn);

    ExecutableWork m4 = createMockWork(key1Shard1, 4, sleepProcessWorkFn);
    assertEquals(0, executor.activeCount());

    assertTrue(computationState.activateWork(m2));
    // activate work starts executing work if no other work is queued for that shard
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(2, executor.activeCount());

    assertTrue(computationState.activateWork(m3));
    assertTrue(computationState.activateWork(m4));
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    processStart2.await();

    assertEquals(3, executor.activeCount());
    executor.execute(m4, m4.getWorkItem().getSerializedSize());
    processStart3.await();
    assertEquals(4, executor.activeCount());
    stop.set(true);
    executor.shutdown();
  }

  @Test
  public void testOutstandingBytesMetric() throws Exception {
    int maxThreads = 5;
    int threadExpirationSec = 60;
    CountDownLatch processStart1 = new CountDownLatch(2);
    CountDownLatch processStart2 = new CountDownLatch(3);
    CountDownLatch processStart3 = new CountDownLatch(4);
    AtomicBoolean stop = new AtomicBoolean(false);
    // setting up actual implementation of executor instead of mocking to keep track of
    // active thread count.
    BoundedQueueExecutor executor =
        new BoundedQueueExecutor(
            maxThreads,
            threadExpirationSec,
            TimeUnit.SECONDS,
            maxThreads,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());

    ComputationState computationState =
        new ComputationState(
            "computation",
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            executor,
            ImmutableMap.of(),
            null);

    ShardedKey key1Shard1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);
    Consumer<Work> sleepProcessWorkFn =
        unused -> {
          processStart1.countDown();
          processStart2.countDown();
          processStart3.countDown();
          int count = 0;
          while (!stop.get()) {
            count += 1;
          }
        };

    ExecutableWork m2 = createMockWork(key1Shard1, 2, sleepProcessWorkFn);

    ExecutableWork m3 = createMockWork(key1Shard1, 3, sleepProcessWorkFn);

    ExecutableWork m4 = createMockWork(key1Shard1, 4, sleepProcessWorkFn);
    assertEquals(0, executor.bytesOutstanding());

    long bytes = m2.getWorkItem().getSerializedSize();
    assertTrue(computationState.activateWork(m2));
    // activate work starts executing work if no other work is queued for that shard
    bytes += m2.getWorkItem().getSerializedSize();
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(bytes, executor.bytesOutstanding());

    assertTrue(computationState.activateWork(m3));
    assertTrue(computationState.activateWork(m4));

    bytes += m3.getWorkItem().getSerializedSize();
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    processStart2.await();
    assertEquals(bytes, executor.bytesOutstanding());

    bytes += m4.getWorkItem().getSerializedSize();
    executor.execute(m4, m4.getWorkItem().getSerializedSize());
    processStart3.await();
    assertEquals(bytes, executor.bytesOutstanding());
    stop.set(true);
    executor.shutdown();
  }

  @Test
  public void testOutstandingBundlesMetric() throws Exception {
    int maxThreads = 5;
    int threadExpirationSec = 60;
    CountDownLatch processStart1 = new CountDownLatch(2);
    CountDownLatch processStart2 = new CountDownLatch(3);
    CountDownLatch processStart3 = new CountDownLatch(4);
    AtomicBoolean stop = new AtomicBoolean(false);
    // setting up actual implementation of executor instead of mocking to keep track of
    // active thread count.
    BoundedQueueExecutor executor =
        new BoundedQueueExecutor(
            maxThreads,
            threadExpirationSec,
            TimeUnit.SECONDS,
            maxThreads,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());

    ComputationState computationState =
        new ComputationState(
            "computation",
            defaultMapTask(Collections.singletonList(makeSourceInstruction(StringUtf8Coder.of()))),
            executor,
            ImmutableMap.of(),
            null);

    ShardedKey key1Shard1 = ShardedKey.create(ByteString.copyFromUtf8("key1"), 1);
    Consumer<Work> sleepProcessWorkFn =
        unused -> {
          processStart1.countDown();
          processStart2.countDown();
          processStart3.countDown();
          int count = 0;
          while (!stop.get()) {
            count += 1;
          }
        };

    ExecutableWork m2 = createMockWork(key1Shard1, 2, sleepProcessWorkFn);

    ExecutableWork m3 = createMockWork(key1Shard1, 3, sleepProcessWorkFn);

    ExecutableWork m4 = createMockWork(key1Shard1, 4, sleepProcessWorkFn);
    assertEquals(0, executor.elementsOutstanding());

    assertTrue(computationState.activateWork(m2));
    // activate work starts executing work if no other work is queued for that shard
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(2, executor.elementsOutstanding());

    assertTrue(computationState.activateWork(m3));
    assertTrue(computationState.activateWork(m4));

    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    processStart2.await();
    assertEquals(3, executor.elementsOutstanding());

    executor.execute(m4, m4.getWorkItem().getSerializedSize());
    processStart3.await();
    assertEquals(4, executor.elementsOutstanding());
    stop.set(true);
    executor.shutdown();
  }

  @Test
  public void testExceptionInvalidatesCache() throws Exception {
    // We'll need to force the system to limit bundles to one message at a time.
    // Sequence is as follows:
    // 01. GetWork[0] (token 0)
    // 02. Create counter reader
    // 03. Counter yields 0
    // 04. GetData[0] (state as null)
    // 05. Read state as null
    // 06. Set state as 42
    // 07. THROW on taking counter reader checkpoint
    // 08. Create counter reader
    // 09. Counter yields 0
    // 10. GetData[1] (state as null)
    // 11. Read state as null (*** not 42 ***)
    // 12. Take counter reader checkpoint as 0
    // 13. CommitWork[0] (message 0:0, state 42, checkpoint 0)
    // 14. GetWork[1] (token 1, checkpoint as 0)
    // 15. Counter yields 1
    // 16. Read (cached) state as 42
    // 17. Take counter reader checkpoint 1
    // 18. CommitWork[1] (message 0:1, checkpoint 1)
    // 19. GetWork[2] (token 2, checkpoint as 1)
    // 20. Counter yields 2
    // 21. THROW on processElement
    // 22. Recreate reader from checkpoint 1
    // 23. Counter yields 2 (*** not eof ***)
    // 24. GetData[2] (state as 42)
    // 25. Read state as 42
    // 26. Take counter reader checkpoint 2
    // 27. CommitWork[2] (message 0:2, checkpoint 2)

    server.setExpectedExceptionCount(2);

    DataflowPipelineOptions options = createTestingPipelineOptions();
    options.setNumWorkers(1);
    options.setUnboundedReaderMaxElements(1);

    CloudObject codec =
        CloudObjects.asCloudObject(
            WindowedValue.getFullCoder(
                ValueWithRecordId.ValueWithRecordIdCoder.of(
                    KvCoder.of(VarIntCoder.of(), VarIntCoder.of())),
                GlobalWindow.Coder.INSTANCE),
            /* sdkComponents= */ null);

    TestCountingSource counter = new TestCountingSource(3).withThrowOnFirstSnapshot(true);

    List<ParallelInstruction> instructions =
        Arrays.asList(
            new ParallelInstruction()
                .setOriginalName("OriginalReadName")
                .setSystemName("Read")
                .setName(DEFAULT_PARDO_USER_NAME)
                .setRead(
                    new ReadInstruction()
                        .setSource(
                            CustomSources.serializeToCloudSource(counter, options).setCodec(codec)))
                .setOutputs(
                    Collections.singletonList(
                        new InstructionOutput()
                            .setName("read_output")
                            .setOriginalName(DEFAULT_OUTPUT_ORIGINAL_NAME)
                            .setSystemName(DEFAULT_OUTPUT_SYSTEM_NAME)
                            .setCodec(codec))),
            makeDoFnInstruction(
                new TestExceptionInvalidatesCacheFn(),
                0,
                StringUtf8Coder.of(),
                WindowingStrategy.globalDefault()),
            makeSinkInstruction(StringUtf8Coder.of(), 1, GlobalWindow.Coder.INSTANCE));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(instructions)
                .setOptions(options.as(DataflowWorkerHarnessOptions.class))
                .setLocalRetryTimeoutMs(100)
                .publishCounters()
                .build());
    worker.start();

    // Three GetData requests
    for (int i = 0; i < 3; i++) {
      ByteString state;
      if (i == 0 || i == 1) {
        state = ByteString.EMPTY;
      } else {
        state = ByteString.copyFrom(new byte[] {42});
      }
      Windmill.GetDataResponse.Builder dataResponse = Windmill.GetDataResponse.newBuilder();
      dataResponse
          .addDataBuilder()
          .setComputationId(DEFAULT_COMPUTATION_ID)
          .addDataBuilder()
          .setKey(ByteString.copyFromUtf8("0000000000000001"))
          .setShardingKey(1)
          .addValuesBuilder()
          .setTag(ByteString.copyFromUtf8("//+uint"))
          .setStateFamily(DEFAULT_PARDO_STATE_FAMILY)
          .getValueBuilder()
          .setTimestamp(0)
          .setData(state);
      server.whenGetDataCalled().thenReturn(dataResponse.build());
    }

    // Three GetWork requests and commits
    for (int i = 0; i < 3; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append("work {\n");
      sb.append("  computation_id: \"computation\"\n");
      sb.append("  input_data_watermark: 0\n");
      sb.append("  work {\n");
      sb.append("    key: \"0000000000000001\"\n");
      sb.append("    sharding_key: 1\n");
      sb.append("    work_token: ");
      sb.append(i);
      sb.append("    cache_token: 1");
      sb.append("\n");
      if (i > 0) {
        int previousCheckpoint = i - 1;
        sb.append("    source_state {\n");
        sb.append("      state: \"");
        sb.append((char) previousCheckpoint);
        sb.append("\"\n");
        // We'll elide the finalize ids since it's not necessary to trigger the finalizer
        // for this test.
        sb.append("    }\n");
      }
      sb.append("  }\n");
      sb.append("}\n");

      server.whenGetWorkCalled().thenReturn(buildInput(sb.toString(), null));

      Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

      Windmill.WorkItemCommitRequest commit = result.get((long) i);
      UnsignedLong finalizeId =
          UnsignedLong.fromLongBits(commit.getSourceStateUpdates().getFinalizeIds(0));

      sb = new StringBuilder();
      sb.append("key: \"0000000000000001\"\n");
      sb.append("sharding_key: 1\n");
      sb.append("work_token: ");
      sb.append(i);
      sb.append("\n");
      sb.append("cache_token: 1\n");
      sb.append("output_messages {\n");
      sb.append("  destination_stream_id: \"out\"\n");
      sb.append("  bundles {\n");
      sb.append("    key: \"0000000000000001\"\n");

      int messageNum = i;
      sb.append("    messages {\n");
      sb.append("      timestamp: ");
      sb.append(messageNum * 1000);
      sb.append("\n");
      sb.append("      data: \"0:");
      sb.append(messageNum);
      sb.append("\"\n");
      sb.append("    }\n");

      sb.append("    messages_ids: \"\"\n");
      sb.append("  }\n");
      sb.append("}\n");
      if (i == 0) {
        sb.append("value_updates {\n");
        sb.append("  tag: \"//+uint\"\n");
        sb.append("  value {\n");
        sb.append("    timestamp: 0\n");
        sb.append("    data: \"");
        sb.append((char) 42);
        sb.append("\"\n");
        sb.append("  }\n");
        sb.append("  state_family: \"parDoStateFamily\"\n");
        sb.append("}\n");
      }

      int sourceState = i;
      sb.append("source_state_updates {\n");
      sb.append("  state: \"");
      sb.append((char) sourceState);
      sb.append("\"\n");
      sb.append("  finalize_ids: ");
      sb.append(finalizeId);
      sb.append("}\n");
      sb.append("source_watermark: ");
      sb.append((sourceState + 1) * 1000);
      sb.append("\n");
      sb.append("source_backlog_bytes: 7\n");

      assertThat(
          // The commit will include a timer to clean up state - this timer is irrelevant
          // for the current test. Also remove source_bytes_processed because it's dynamic.
          setValuesTimestamps(
                  removeDynamicFields(commit)
                      .toBuilder()
                      .clearOutputTimers()
                      .clearSourceBytesProcessed())
              .build(),
          equalTo(
              setMessagesMetadata(
                      PaneInfo.NO_FIRING,
                      CoderUtils.encodeToByteArray(
                          CollectionCoder.of(GlobalWindow.Coder.INSTANCE),
                          ImmutableList.of(GlobalWindow.INSTANCE)),
                      parseCommitRequest(sb.toString()))
                  .build()));
    }
  }

  @Test
  public void testHugeCommits() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new FanoutFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    server.whenGetWorkCalled().thenReturn(makeInput(0, TimeUnit.MILLISECONDS.toMicros(0)));

    server.waitForAndGetCommits(0);
    worker.stop();
  }

  @Test
  public void testActiveWorkRefresh() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new SlowDoFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    server.whenGetWorkCalled().thenReturn(makeInput(0, TimeUnit.MILLISECONDS.toMicros(0)));
    server.waitForAndGetCommits(1);

    worker.stop();

    // This graph will not normally produce any GetData calls, so all such calls are from active
    // work refreshes.
    assertThat(server.numGetDataRequests(), greaterThan(0));
  }

  @Test
  public void testActiveWorkFailure() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(blockingFn, 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    GetWorkResponse workItem =
        makeInput(0, TimeUnit.MILLISECONDS.toMicros(0), "key", DEFAULT_SHARDING_KEY);
    int failedWorkToken = 1;
    int failedCacheToken = 5;
    GetWorkResponse workItemToFail =
        makeInput(
            failedWorkToken,
            failedCacheToken,
            TimeUnit.MILLISECONDS.toMicros(0),
            "key",
            DEFAULT_SHARDING_KEY);

    // Queue up two work items for the same key.
    server.whenGetWorkCalled().thenReturn(workItem).thenReturn(workItemToFail);
    server.waitForEmptyWorkQueue();

    // Mock Windmill sending a heartbeat response failing the second work item while the first
    // is still processing.
    ComputationHeartbeatResponse.Builder failedHeartbeat =
        ComputationHeartbeatResponse.newBuilder();
    failedHeartbeat
        .setComputationId(DEFAULT_COMPUTATION_ID)
        .addHeartbeatResponsesBuilder()
        .setCacheToken(failedCacheToken)
        .setWorkToken(failedWorkToken)
        .setShardingKey(DEFAULT_SHARDING_KEY)
        .setFailed(true);
    server.sendFailedHeartbeats(Collections.singletonList(failedHeartbeat.build()));

    // Release the blocked calls.
    BlockingFn.blocker.countDown();
    Map<Long, Windmill.WorkItemCommitRequest> commits =
        server.waitForAndGetCommitsWithTimeout(2, Duration.standardSeconds((5)));
    assertEquals(1, commits.size());

    worker.stop();
  }

  @Test
  public void testLatencyAttributionProtobufsPopulated() {
    FakeClock clock = new FakeClock();
    Work work =
        Work.create(
            Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(1L).build(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            clock,
            Collections.emptyList());

    clock.sleep(Duration.millis(10));
    work.setState(Work.State.PROCESSING);
    clock.sleep(Duration.millis(20));
    work.setState(Work.State.READING);
    clock.sleep(Duration.millis(30));
    work.setState(Work.State.PROCESSING);
    clock.sleep(Duration.millis(40));
    work.setState(Work.State.COMMIT_QUEUED);
    clock.sleep(Duration.millis(50));
    work.setState(Work.State.COMMITTING);
    clock.sleep(Duration.millis(60));

    Iterator<LatencyAttribution> it =
        work.getLatencyAttributions(DataflowExecutionStateSampler.instance()).iterator();
    assertTrue(it.hasNext());
    LatencyAttribution lat = it.next();
    assertSame(State.QUEUED, lat.getState());
    assertEquals(10, lat.getTotalDurationMillis());
    assertTrue(it.hasNext());
    lat = it.next();
    assertSame(State.ACTIVE, lat.getState());
    assertEquals(60, lat.getTotalDurationMillis());
    assertTrue(it.hasNext());
    lat = it.next();
    assertSame(State.READING, lat.getState());
    assertEquals(30, lat.getTotalDurationMillis());
    assertTrue(it.hasNext());
    lat = it.next();
    assertSame(State.COMMITTING, lat.getState());
    assertEquals(110, lat.getTotalDurationMillis());
    assertFalse(it.hasNext());
  }

  @Test
  public void testLatencyAttributionToQueuedState() throws Exception {
    final int workToken = 3232; // A unique id makes it easier to search logs.

    FakeClock clock = new FakeClock();
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(
                new FakeSlowDoFn(clock, Duration.millis(1000)), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams(
                    "--activeWorkRefreshPeriodMillis=100",
                    // A single-threaded worker processes work sequentially, leaving a second work
                    // item in state QUEUED until the first work item is committed.
                    "--numberOfWorkerHarnessThreads=1")
                .setInstructions(instructions)
                .setClock(clock)
                .setExecutorSupplier(clock::newFakeScheduledExecutor)
                .build());
    worker.start();

    ActiveWorkRefreshSink awrSink = new ActiveWorkRefreshSink(EMPTY_DATA_RESPONDER);
    server.whenGetDataCalled().answerByDefault(awrSink::getData).delayEachResponseBy(Duration.ZERO);
    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(workToken + 1, 0 /* timestamp */))
        .thenReturn(makeInput(workToken, 1 /* timestamp */));
    server.waitForAndGetCommits(2);

    worker.stop();

    assertEquals(
        awrSink.getLatencyAttributionDuration(workToken, State.QUEUED), Duration.millis(1000));
    assertEquals(awrSink.getLatencyAttributionDuration(workToken + 1, State.QUEUED), Duration.ZERO);
  }

  @Test
  public void testLatencyAttributionToActiveState() throws Exception {
    final int workToken = 4242; // A unique id makes it easier to search logs.

    FakeClock clock = new FakeClock();
    // Inject processing latency on the fake clock in the worker via FakeSlowDoFn.
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(
                new FakeSlowDoFn(clock, Duration.millis(1000)), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .setClock(clock)
                .setExecutorSupplier(clock::newFakeScheduledExecutor)
                .build());
    worker.start();

    ActiveWorkRefreshSink awrSink = new ActiveWorkRefreshSink(EMPTY_DATA_RESPONDER);
    server.whenGetDataCalled().answerByDefault(awrSink::getData).delayEachResponseBy(Duration.ZERO);
    server.whenGetWorkCalled().thenReturn(makeInput(workToken, 0 /* timestamp */));
    server.waitForAndGetCommits(1);

    worker.stop();

    assertEquals(
        awrSink.getLatencyAttributionDuration(workToken, State.ACTIVE), Duration.millis(1000));
  }

  @Test
  public void testLatencyAttributionToReadingState() throws Exception {
    final int workToken = 5454; // A unique id makes it easier to find logs.

    FakeClock clock = new FakeClock();
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new ReadingDoFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .setClock(clock)
                .setExecutorSupplier(clock::newFakeScheduledExecutor)
                .build());
    worker.start();

    // Inject latency on the fake clock when the server receives a GetData call that isn't
    // only for refreshing active work.
    ActiveWorkRefreshSink awrSink =
        new ActiveWorkRefreshSink(
            (request) -> {
              clock.sleep(Duration.millis(1000));
              return EMPTY_DATA_RESPONDER.apply(request);
            });
    server.whenGetDataCalled().answerByDefault(awrSink::getData).delayEachResponseBy(Duration.ZERO);
    server.whenGetWorkCalled().thenReturn(makeInput(workToken, 0 /* timestamp */));
    server.waitForAndGetCommits(1);

    worker.stop();

    assertEquals(
        awrSink.getLatencyAttributionDuration(workToken, State.READING), Duration.millis(1000));
  }

  @Test
  public void testLatencyAttributionToCommittingState() throws Exception {
    final int workToken = 6464; // A unique id makes it easier to find logs.

    FakeClock clock = new FakeClock();
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    // Inject latency on the fake clock when the server receives a CommitWork call.

    server
        .whenCommitWorkCalled()
        .answerByDefault(
            (request) -> {
              clock.sleep(Duration.millis(1000));
              return Windmill.CommitWorkResponse.getDefaultInstance();
            });

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .setClock(clock)
                .setExecutorSupplier(clock::newFakeScheduledExecutor)
                .build());
    worker.start();

    ActiveWorkRefreshSink awrSink = new ActiveWorkRefreshSink(EMPTY_DATA_RESPONDER);
    server.whenGetDataCalled().answerByDefault(awrSink::getData).delayEachResponseBy(Duration.ZERO);
    server.whenGetWorkCalled().thenReturn(makeInput(workToken, TimeUnit.MILLISECONDS.toMicros(0)));
    server.waitForAndGetCommits(1);

    worker.stop();

    assertEquals(
        awrSink.getLatencyAttributionDuration(workToken, State.COMMITTING), Duration.millis(1000));
  }

  @Test
  public void testLatencyAttributionPopulatedInCommitRequest() throws Exception {
    final int workToken = 7272; // A unique id makes it easier to search logs.

    long dofnWaitTimeMs = 1000;
    FakeClock clock = new FakeClock();
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(
                new FakeSlowDoFn(clock, Duration.millis(dofnWaitTimeMs)), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .setClock(clock)
                .setExecutorSupplier(clock::newFakeScheduledExecutor)
                .build());
    worker.start();

    ActiveWorkRefreshSink awrSink = new ActiveWorkRefreshSink(EMPTY_DATA_RESPONDER);
    server.whenGetDataCalled().answerByDefault(awrSink::getData).delayEachResponseBy(Duration.ZERO);
    server.whenGetWorkCalled().thenReturn(makeInput(workToken, 1 /* timestamp */));
    Map<Long, WorkItemCommitRequest> workItemCommitRequest = server.waitForAndGetCommits(1);

    worker.stop();

    LatencyAttribution.Builder expectedActiveLA =
        LatencyAttribution.newBuilder()
            .setState(State.ACTIVE)
            .setTotalDurationMillis(dofnWaitTimeMs);
    assertThat(
        workItemCommitRequest.get((long) workToken).getPerWorkItemLatencyAttributions(0),
        hasProperty("state", Matchers.equalTo(State.ACTIVE)));
    assertThat(
        workItemCommitRequest.get((long) workToken).getPerWorkItemLatencyAttributions(0),
        hasProperty("totalDurationMillis", Matchers.equalTo(1000L)));
    assertThat(
        workItemCommitRequest.get((long) workToken).getPerWorkItemLatencyAttributions(0),
        hasProperty("activeLatencyBreakdown"));
    if (streamingEngine) {
      // Initial fake latency provided to FakeWindmillServer when invoke receiveWork in
      // GetWorkStream().
      assertEquals(
          workItemCommitRequest.get((long) workToken).getPerWorkItemLatencyAttributions(1),
          LatencyAttribution.newBuilder()
              .setState(State.GET_WORK_IN_TRANSIT_TO_USER_WORKER)
              .setTotalDurationMillis(1000)
              .build());
    }
  }

  @Test
  public void testDoFnLatencyBreakdownsReportedOnCommit() throws Exception {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new SlowDoFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=100")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    server.whenGetWorkCalled().thenReturn(makeInput(0, TimeUnit.MILLISECONDS.toMicros(0)));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Windmill.WorkItemCommitRequest commit = result.get(0L);

    Windmill.LatencyAttribution.Builder laBuilder =
        LatencyAttribution.newBuilder().setState(State.ACTIVE).setTotalDurationMillis(100);
    for (LatencyAttribution la : commit.getPerWorkItemLatencyAttributionsList()) {
      if (la.getState() == State.ACTIVE) {
        assertThat(la.getActiveLatencyBreakdownCount(), equalTo(1));
        assertThat(
            la.getActiveLatencyBreakdown(0).getUserStepName(), equalTo(DEFAULT_PARDO_USER_NAME));
        Assert.assertTrue(la.getActiveLatencyBreakdown(0).hasProcessingTimesDistribution());
        Assert.assertFalse(la.getActiveLatencyBreakdown(0).hasActiveMessageMetadata());
      }
    }

    worker.stop();
  }

  @Test
  public void testDoFnActiveMessageMetadataReportedOnHeartbeat() throws Exception {
    if (!streamingEngine) {
      return;
    }
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeDoFnInstruction(new SlowDoFn(), 0, StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--activeWorkRefreshPeriodMillis=10")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();

    server.whenGetWorkCalled().thenReturn(makeInput(0, TimeUnit.MILLISECONDS.toMicros(0)));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);

    assertThat(server.numGetDataRequests(), greaterThan(0));
    Windmill.GetDataRequest heartbeat = server.getGetDataRequests().get(1);

    for (LatencyAttribution la :
        heartbeat
            .getComputationHeartbeatRequest(0)
            .getHeartbeatRequests(0)
            .getLatencyAttributionList()) {
      if (la.getState() == State.ACTIVE) {
        assertTrue(la.getActiveLatencyBreakdownCount() > 0);
        assertTrue(la.getActiveLatencyBreakdown(0).hasActiveMessageMetadata());
      }
    }

    worker.stop();
  }

  @Test
  public void testLimitOnOutputBundleSize() throws Exception {
    // This verifies that ReadOperation, StreamingModeExecutionContext, and windmill sinks
    // coordinate to limit size of an output bundle.
    List<Integer> finalizeTracker = Lists.newArrayList();
    TestCountingSource.setFinalizeTracker(finalizeTracker);

    final int numMessagesInCustomSourceShard = 100000; // 100K input messages.
    final int inflatedSizePerMessage = 10000; // x10k => 1GB total output size.

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams()
                .setInstructions(
                    makeUnboundedSourcePipeline(
                        numMessagesInCustomSourceShard, new InflateDoFn(inflatedSizePerMessage)))
                .build());
    worker.start();

    // Test new key.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 1"
                    + "    cache_token: 1"
                    + "  }"
                    + "}",
                null));

    // Matcher to ensure that commit size is within 10% of max bundle size.
    Matcher<Integer> isWithinBundleSizeLimits =
        both(greaterThan(StreamingDataflowWorker.MAX_SINK_BYTES * 9 / 10))
            .and(lessThan(StreamingDataflowWorker.MAX_SINK_BYTES * 11 / 10));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Windmill.WorkItemCommitRequest commit = result.get(1L);

    assertThat(commit.getSerializedSize(), isWithinBundleSizeLimits);

    // Try another bundle
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 2"
                    + "    cache_token: 1"
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);
    commit = result.get(2L);

    assertThat(commit.getSerializedSize(), isWithinBundleSizeLimits);
  }

  @Test
  public void testLimitOnOutputBundleSizeWithMultipleSinks() throws Exception {
    // Same as testLimitOnOutputBundleSize(), but with 3 sinks for the stage rather than one.
    // Verifies that output bundle size has same limit even with multiple sinks.
    List<Integer> finalizeTracker = Lists.newArrayList();
    TestCountingSource.setFinalizeTracker(finalizeTracker);

    final int numMessagesInCustomSourceShard = 100000; // 100K input messages.
    final int inflatedSizePerMessage = 10000; // x10k => 1GB total output size.

    List<ParallelInstruction> instructions = new ArrayList<>();
    instructions.addAll(
        makeUnboundedSourcePipeline(
            numMessagesInCustomSourceShard, new InflateDoFn(inflatedSizePerMessage)));
    // add two more sinks
    instructions.add(
        makeSinkInstruction(
            DEFAULT_DESTINATION_STREAM_ID + "-1",
            StringUtf8Coder.of(),
            1,
            GlobalWindow.Coder.INSTANCE));
    instructions.add(
        makeSinkInstruction(
            DEFAULT_DESTINATION_STREAM_ID + "-2",
            StringUtf8Coder.of(),
            1,
            GlobalWindow.Coder.INSTANCE));
    StreamingDataflowWorker worker =
        makeWorker(defaultWorkerParams().setInstructions(instructions).publishCounters().build());
    worker.start();

    // Test new key.
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 1"
                    + "    cache_token: 1"
                    + "  }"
                    + "}",
                null));

    // Matcher to ensure that commit size is within 10% of max bundle size.
    Matcher<Integer> isWithinBundleSizeLimits =
        both(greaterThan(StreamingDataflowWorker.MAX_SINK_BYTES * 9 / 10))
            .and(lessThan(StreamingDataflowWorker.MAX_SINK_BYTES * 11 / 10));

    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    Windmill.WorkItemCommitRequest commit = result.get(1L);

    assertThat(commit.getSerializedSize(), isWithinBundleSizeLimits);

    // Try another bundle
    server
        .whenGetWorkCalled()
        .thenReturn(
            buildInput(
                "work {"
                    + "  computation_id: \"computation\""
                    + "  input_data_watermark: 0"
                    + "  work {"
                    + "    key: \"0000000000000001\""
                    + "    sharding_key: 1"
                    + "    work_token: 2"
                    + "    cache_token: 1"
                    + "  }"
                    + "}",
                null));

    result = server.waitForAndGetCommits(1);
    commit = result.get(2L);

    assertThat(commit.getSerializedSize(), isWithinBundleSizeLimits);
  }

  @Test
  public void testStuckCommit() throws Exception {
    if (!streamingEngine) {
      // Stuck commits have only been observed with streaming engine and thus recovery from them is
      // not implemented for non-streaming engine.
      return;
    }

    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--stuckCommitDurationMillis=2000")
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();
    // Prevent commit callbacks from being called to simulate a stuck commit.
    server.setDropStreamingCommits(true);

    // Add some work for key 1.
    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(10, TimeUnit.MILLISECONDS.toMicros(2), DEFAULT_KEY_STRING, 1))
        .thenReturn(makeInput(15, TimeUnit.MILLISECONDS.toMicros(3), DEFAULT_KEY_STRING, 5));
    ConcurrentHashMap<Long, Consumer<CommitStatus>> droppedCommits =
        server.waitForDroppedCommits(2);
    server.setDropStreamingCommits(false);
    // Enqueue another work item for key 1.
    server
        .whenGetWorkCalled()
        .thenReturn(makeInput(1, TimeUnit.MILLISECONDS.toMicros(1), DEFAULT_KEY_STRING, 1));
    // Ensure that this work item processes.
    Map<Long, Windmill.WorkItemCommitRequest> result = server.waitForAndGetCommits(1);
    // Now ensure that nothing happens if a dropped commit actually completes.
    droppedCommits.values().iterator().next().accept(CommitStatus.OK);
    worker.stop();

    assertTrue(result.containsKey(1L));
    assertEquals(
        makeExpectedOutput(
                1, TimeUnit.MILLISECONDS.toMicros(1), DEFAULT_KEY_STRING, 1, DEFAULT_KEY_STRING)
            .build(),
        removeDynamicFields(result.get(1L)));
  }

  private void runNumCommitThreadsTest(int configNumCommitThreads, int expectedNumCommitThreads) {
    List<ParallelInstruction> instructions =
        Arrays.asList(
            makeSourceInstruction(StringUtf8Coder.of()),
            makeSinkInstruction(StringUtf8Coder.of(), 0));

    StreamingDataflowWorker worker =
        makeWorker(
            defaultWorkerParams("--windmillServiceCommitThreads=" + configNumCommitThreads)
                .setInstructions(instructions)
                .publishCounters()
                .build());
    worker.start();
    assertEquals(expectedNumCommitThreads, worker.numCommitThreads());
    worker.stop();
  }

  @Test
  public void testDefaultNumCommitThreads() {
    if (streamingEngine) {
      runNumCommitThreadsTest(1, 1);
      runNumCommitThreadsTest(2, 2);
      runNumCommitThreadsTest(3, 3);
      runNumCommitThreadsTest(0, 1);
      runNumCommitThreadsTest(-1, 1);
    } else {
      runNumCommitThreadsTest(1, 1);
      runNumCommitThreadsTest(2, 1);
      runNumCommitThreadsTest(3, 1);
      runNumCommitThreadsTest(0, 1);
      runNumCommitThreadsTest(-1, 1);
    }
  }

  static class BlockingFn extends DoFn<String, String> implements TestRule {

    public static CountDownLatch blocker = new CountDownLatch(1);
    public static Semaphore counter = new Semaphore(0);
    public static AtomicInteger callCounter = new AtomicInteger(0);

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      callCounter.incrementAndGet();
      counter.release();
      blocker.await();
      c.output(c.element());
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          blocker = new CountDownLatch(1);
          counter = new Semaphore(0);
          callCounter = new AtomicInteger();
          base.evaluate();
        }
      };
    }
  }

  static class KeyTokenInvalidFn extends DoFn<KV<String, String>, KV<String, String>> {

    static boolean thrown = false;

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (!thrown) {
        thrown = true;
        throw new KeyTokenInvalidException("key");
      } else {
        c.output(c.element());
      }
    }
  }

  static class LargeCommitFn extends DoFn<KV<String, String>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().getKey().equals("large_key")) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 100; ++i) {
          s.append("large_commit");
        }
        c.output(KV.of(c.element().getKey(), s.toString()));
      } else {
        c.output(c.element());
      }
    }
  }

  static class ExceptionCatchingFn extends DoFn<KV<String, String>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        c.output(KV.of(c.element().getKey(), c.element().getValue() + "_a_bunch_more_data_output"));
      } catch (Exception e) {
        c.output(KV.of("smaller_key", c.element().getValue()));
      }
    }
  }

  static class ChangeKeysFn extends DoFn<KV<String, String>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, String> elem = c.element();
      c.output(KV.of(elem.getKey() + "_" + elem.getValue(), elem.getValue()));
    }
  }

  static class TestExceptionFn extends DoFn<String, String> {

    boolean firstTime = true;

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if (firstTime) {
        firstTime = false;
        try {
          throw new Exception("Exception!");
        } catch (Exception e) {
          throw new Exception("Another exception!", e);
        }
      }
    }
  }

  static class PassthroughDoFn
      extends DoFn<KV<String, Iterable<String>>, KV<String, Iterable<String>>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  static class Action {

    GetWorkResponse response;
    Timer[] expectedTimers = new Timer[] {};
    WatermarkHold[] expectedHolds = new WatermarkHold[] {};

    public Action(GetWorkResponse response) {
      this.response = response;
    }

    Action withHolds(WatermarkHold... holds) {
      this.expectedHolds = holds;
      return this;
    }

    Action withTimers(Timer... timers) {
      this.expectedTimers = timers;
      return this;
    }
  }

  static class PrintFn extends DoFn<ValueWithRecordId<KV<Integer, Integer>>, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, Integer> elem = c.element().getValue();
      c.output(elem.getKey() + ":" + elem.getValue());
    }
  }

  static class TestExceptionInvalidatesCacheFn
      extends DoFn<ValueWithRecordId<KV<Integer, Integer>>, String> {

    static boolean thrown = false;

    @StateId("int")
    private final StateSpec<ValueState<Integer>> counter = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("int") ValueState<Integer> state)
        throws Exception {
      KV<Integer, Integer> elem = c.element().getValue();
      if (elem.getValue() == 0) {
        LOG.error("**** COUNTER 0 ****");
        assertNull(state.read());
        state.write(42);
        assertEquals((Integer) 42, state.read());
      } else if (elem.getValue() == 1) {
        LOG.error("**** COUNTER 1 ****");
        assertEquals((Integer) 42, state.read());
      } else if (elem.getValue() == 2) {
        if (!thrown) {
          LOG.error("**** COUNTER 2 (will throw) ****");
          thrown = true;
          throw new Exception("Exception!");
        }
        LOG.error("**** COUNTER 2 (retry) ****");
        assertEquals((Integer) 42, state.read());
      } else {
        throw new RuntimeException("only expecting values [0,2]");
      }
      c.output(elem.getKey() + ":" + elem.getValue());
    }
  }

  private static class FanoutFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      StringBuilder builder = new StringBuilder(1000000);
      for (int i = 0; i < 1000000; i++) {
        builder.append(' ');
      }
      String largeString = builder.toString();
      for (int i = 0; i < 3000; i++) {
        c.output(largeString);
      }
    }
  }

  private static class SlowDoFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Thread.sleep(1000);
      c.output(c.element());
    }
  }

  static class FakeClock implements Supplier<Instant> {
    private final PriorityQueue<Job> jobs = new PriorityQueue<>();
    private Instant now = Instant.now();

    public ScheduledExecutorService newFakeScheduledExecutor(String unused) {
      return new FakeScheduledExecutor();
    }

    @Override
    public synchronized Instant get() {
      return now;
    }

    public synchronized void clear() {
      jobs.clear();
    }

    public synchronized void sleep(Duration duration) {
      if (duration.isShorterThan(Duration.ZERO)) {
        throw new UnsupportedOperationException("Cannot sleep backwards in time");
      }
      Instant endOfSleep = now.plus(duration);
      while (true) {
        Job job = jobs.peek();
        if (job == null || job.when.isAfter(endOfSleep)) {
          break;
        }
        jobs.remove();
        now = job.when;
        job.work.run();
      }
      now = endOfSleep;
    }

    private synchronized void schedule(Duration fromNow, Runnable work) {
      jobs.add(new Job(now.plus(fromNow), work));
    }

    private static class Job implements Comparable<Job> {
      final Instant when;
      final Runnable work;

      Job(Instant when, Runnable work) {
        this.when = when;
        this.work = work;
      }

      @Override
      public int compareTo(Job job) {
        return when.compareTo(job.when);
      }
    }

    private class FakeScheduledExecutor implements ScheduledExecutorService {
      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
      }

      @Override
      public void execute(Runnable command) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
          throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> List<Future<T>> invokeAll(
          Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
          throws ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws ExecutionException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public boolean isShutdown() {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public boolean isTerminated() {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public void shutdown() {}

      @Override
      public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public ScheduledFuture<?> scheduleAtFixedRate(
          Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public ScheduledFuture<?> scheduleWithFixedDelay(
          Runnable command, long initialDelay, long delay, TimeUnit unit) {
        if (delay <= 0) {
          throw new UnsupportedOperationException(
              "Please supply a delay > 0 to scheduleWithFixedDelay");
        }
        FakeClock.this.schedule(
            Duration.millis(unit.toMillis(initialDelay)),
            new Runnable() {
              @Override
              public void run() {
                command.run();
                FakeClock.this.schedule(Duration.millis(unit.toMillis(delay)), this);
              }
            });
        FakeClock.this.sleep(Duration.ZERO); // Execute work that has an initial delay of zero.
        return null;
      }
    }
  }

  private static class FakeSlowDoFn extends DoFn<String, String> {
    private static FakeClock clock; // A static variable keeps this DoFn serializable.
    private final Duration sleep;

    FakeSlowDoFn(FakeClock clock, Duration sleep) {
      FakeSlowDoFn.clock = clock;
      this.sleep = sleep;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      clock.sleep(sleep);
      c.output(c.element());
    }
  }

  // Aggregates LatencyAttribution data from active work refresh requests.
  static class ActiveWorkRefreshSink {
    private final Function<GetDataRequest, GetDataResponse> responder;
    private final Map<Long, EnumMap<LatencyAttribution.State, Duration>> totalDurations =
        new HashMap<>();

    ActiveWorkRefreshSink(Function<GetDataRequest, GetDataResponse> responder) {
      this.responder = responder;
    }

    Duration getLatencyAttributionDuration(long workToken, LatencyAttribution.State state) {
      EnumMap<LatencyAttribution.State, Duration> durations = totalDurations.get(workToken);
      return durations == null ? Duration.ZERO : durations.getOrDefault(state, Duration.ZERO);
    }

    boolean isActiveWorkRefresh(GetDataRequest request) {
      if (request.getComputationHeartbeatRequestCount() > 0) {
        return true;
      }
      for (ComputationGetDataRequest computationRequest : request.getRequestsList()) {
        if (!computationRequest.getComputationId().equals(DEFAULT_COMPUTATION_ID)) {
          return false;
        }
        for (KeyedGetDataRequest keyedRequest : computationRequest.getRequestsList()) {
          if (keyedRequest.getWorkToken() == 0
              || keyedRequest.getShardingKey() != DEFAULT_SHARDING_KEY
              || keyedRequest.getValuesToFetchCount() != 0
              || keyedRequest.getBagsToFetchCount() != 0
              || keyedRequest.getTagValuePrefixesToFetchCount() != 0
              || keyedRequest.getWatermarkHoldsToFetchCount() != 0) {
            return false;
          }
        }
      }
      return true;
    }

    GetDataResponse getData(GetDataRequest request) {
      if (!isActiveWorkRefresh(request)) {
        return responder.apply(request);
      }
      for (ComputationGetDataRequest computationRequest : request.getRequestsList()) {
        for (KeyedGetDataRequest keyedRequest : computationRequest.getRequestsList()) {
          for (LatencyAttribution la : keyedRequest.getLatencyAttributionList()) {
            EnumMap<LatencyAttribution.State, Duration> durations =
                totalDurations.computeIfAbsent(
                    keyedRequest.getWorkToken(),
                    (Long workToken) ->
                        new EnumMap<LatencyAttribution.State, Duration>(
                            LatencyAttribution.State.class));
            Duration cur = Duration.millis(la.getTotalDurationMillis());
            durations.compute(la.getState(), (s, d) -> d == null || d.isShorterThan(cur) ? cur : d);
          }
        }
      }
      for (ComputationHeartbeatRequest heartbeatRequest :
          request.getComputationHeartbeatRequestList()) {
        for (HeartbeatRequest heartbeat : heartbeatRequest.getHeartbeatRequestsList()) {
          for (LatencyAttribution la : heartbeat.getLatencyAttributionList()) {
            EnumMap<LatencyAttribution.State, Duration> durations =
                totalDurations.computeIfAbsent(
                    heartbeat.getWorkToken(),
                    (Long workToken) ->
                        new EnumMap<LatencyAttribution.State, Duration>(
                            LatencyAttribution.State.class));
            Duration cur = Duration.millis(la.getTotalDurationMillis());
            durations.compute(la.getState(), (s, d) -> d == null || d.isShorterThan(cur) ? cur : d);
          }
        }
      }
      return EMPTY_DATA_RESPONDER.apply(request);
    }
  }

  // A DoFn that triggers a GetData request.
  static class ReadingDoFn extends DoFn<String, String> {
    @StateId("int")
    private final StateSpec<ValueState<Integer>> counter = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("int") ValueState<Integer> state) {
      state.read();
      c.output(c.element());
    }
  }

  /** For each input element, emits a large string. */
  private static class InflateDoFn extends DoFn<ValueWithRecordId<KV<Integer, Integer>>, String> {

    final int inflatedSize;

    /** For each input elements, outputs a string of this length */
    InflateDoFn(int inflatedSize) {
      this.inflatedSize = inflatedSize;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      char[] chars = new char[inflatedSize];
      Arrays.fill(chars, ' ');
      c.output(new String(chars));
    }
  }

  @AutoValue
  abstract static class StreamingDataflowWorkerTestParams {

    private static StreamingDataflowWorkerTestParams.Builder builder() {
      return new AutoValue_StreamingDataflowWorkerTest_StreamingDataflowWorkerTestParams.Builder()
          .addStateNameMapping(DEFAULT_PARDO_USER_NAME, DEFAULT_PARDO_STATE_FAMILY)
          .setExecutorSupplier(ignored -> Executors.newSingleThreadScheduledExecutor())
          .setLocalRetryTimeoutMs(-1)
          .setPublishCounters(false)
          .setClock(Instant::now)
          .setStreamingGlobalConfig(StreamingGlobalConfig.builder().build());
    }

    abstract ImmutableMap<String, String> stateNameMappings();

    abstract List<ParallelInstruction> instructions();

    abstract DataflowWorkerHarnessOptions options();

    abstract boolean publishCounters();

    abstract Supplier<Instant> clock();

    abstract Function<String, ScheduledExecutorService> executorSupplier();

    abstract int localRetryTimeoutMs();

    abstract StreamingGlobalConfig streamingGlobalConfig();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setStateNameMappings(ImmutableMap<String, String> value);

      abstract ImmutableMap.Builder<String, String> stateNameMappingsBuilder();

      final Builder addStateNameMapping(String username, String stateFamilyName) {
        stateNameMappingsBuilder().put(username, stateFamilyName);
        return this;
      }

      final Builder addAllStateNameMappings(Map<String, String> stateNameMappings) {
        stateNameMappingsBuilder().putAll(stateNameMappings);
        return this;
      }

      abstract Builder setInstructions(List<ParallelInstruction> value);

      abstract Builder setOptions(DataflowWorkerHarnessOptions value);

      abstract Builder setPublishCounters(boolean value);

      final Builder publishCounters() {
        return setPublishCounters(true);
      }

      abstract Builder setClock(Supplier<Instant> value);

      abstract Builder setExecutorSupplier(Function<String, ScheduledExecutorService> value);

      abstract Builder setLocalRetryTimeoutMs(int value);

      abstract Builder setStreamingGlobalConfig(StreamingGlobalConfig config);

      abstract StreamingDataflowWorkerTestParams build();
    }
  }
}
