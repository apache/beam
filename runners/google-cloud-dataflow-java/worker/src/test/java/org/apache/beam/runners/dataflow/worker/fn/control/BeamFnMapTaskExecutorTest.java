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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortWriteOperation;
import org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.util.MoreFutures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnMapTaskExecutor}. */
@RunWith(JUnit4.class)
public class BeamFnMapTaskExecutorTest {

  @Mock private OperationContext mockContext;
  @Mock private StateDelegator mockBeamFnStateDelegator;
  @Mock private StateDelegator.Registration mockStateDelegatorRegistration;
  @Mock private ExecutionStateTracker executionStateTracker;

  // Hacks to cause progress tracking to be done
  @Mock private ReadOperation readOperation;
  @Mock private RemoteGrpcPortWriteOperation grpcPortWriteOperation;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockBeamFnStateDelegator.registerForProcessBundleInstructionId(
            any(String.class), any(StateRequestHandler.class)))
        .thenReturn(mockStateDelegatorRegistration);
  }

  private static final String GRPC_READ_ID = "fake_grpc_read_id";
  private static final String FAKE_OUTPUT_NAME = "fake_output_name";
  private static final String FAKE_OUTPUT_PCOLLECTION_ID = "fake_pcollection_id";

  private static final BeamFnApi.Metrics.PTransform FAKE_ELEMENT_COUNT_METRICS =
      BeamFnApi.Metrics.PTransform.newBuilder()
          .setProcessedElements(
              BeamFnApi.Metrics.PTransform.ProcessedElements.newBuilder()
                  .setMeasured(BeamFnApi.Metrics.PTransform.Measured.getDefaultInstance()))
          .build();

  private static final BeamFnApi.RegisterRequest REGISTER_REQUEST =
      BeamFnApi.RegisterRequest.newBuilder()
          .addProcessBundleDescriptor(
              BeamFnApi.ProcessBundleDescriptor.newBuilder()
                  .setId("555")
                  .putTransforms(
                      GRPC_READ_ID,
                      RunnerApi.PTransform.newBuilder()
                          .setSpec(
                              RunnerApi.FunctionSpec.newBuilder().setUrn(RemoteGrpcPortRead.URN))
                          .putOutputs(FAKE_OUTPUT_NAME, FAKE_OUTPUT_PCOLLECTION_ID)
                          .build()))
          .build();

  @Test(timeout = ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS * 10)
  public void testTentativeUserMetrics() throws Exception {
    Supplier<String> idGenerator = makeIdGeneratorStartingFrom(777L);

    final String stepName = "fakeStepNameWithUserMetrics";
    final String namespace = "sdk/whatever";
    final String name = "someCounter";
    final int counterValue = 42;
    final CountDownLatch progressSentLatch = new CountDownLatch(1);
    final CountDownLatch processBundleLatch = new CountDownLatch(1);

    final BeamFnApi.Metrics.User.MetricName metricName =
        BeamFnApi.Metrics.User.MetricName.newBuilder()
            .setNamespace(namespace)
            .setName(name)
            .build();

    InstructionRequestHandler instructionRequestHandler =
        new InstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      processBundleLatch.await();
                      return responseFor(request)
                          .setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance())
                          .build();
                    });
              case PROCESS_BUNDLE_PROGRESS:
                progressSentLatch.countDown();
                return CompletableFuture.completedFuture(
                    responseFor(request)
                        .setProcessBundleProgress(
                            BeamFnApi.ProcessBundleProgressResponse.newBuilder()
                                .setMetrics(
                                    BeamFnApi.Metrics.newBuilder()
                                        .putPtransforms(GRPC_READ_ID, FAKE_ELEMENT_COUNT_METRICS)
                                        .putPtransforms(
                                            stepName,
                                            BeamFnApi.Metrics.PTransform.newBuilder()
                                                .addUser(
                                                    BeamFnApi.Metrics.User.newBuilder()
                                                        .setMetricName(metricName)
                                                        .setCounterData(
                                                            BeamFnApi.Metrics.User.CounterData
                                                                .newBuilder()
                                                                .setValue(counterValue)))
                                                .build())))
                        .build());
              default:
                // block forever
                return new CompletableFuture<>();
            }
          }

          @Override
          public void close() {}
        };

    RegisterAndProcessBundleOperation processOperation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            mockContext);

    BeamFnMapTaskExecutor mapTaskExecutor =
        BeamFnMapTaskExecutor.forOperations(
            ImmutableList.of(readOperation, grpcPortWriteOperation, processOperation),
            executionStateTracker);

    // Launch the BeamFnMapTaskExecutor and wait until we are sure there has been one
    // tentative update
    CompletionStage<Void> doneFuture = MoreFutures.runAsync(mapTaskExecutor::execute);
    progressSentLatch.await();

    Iterable<CounterUpdate> metricsCounterUpdates = Collections.emptyList();
    while (Iterables.size(metricsCounterUpdates) == 0) {
      Thread.sleep(ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS);
      metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();
    }

    assertThat(
        metricsCounterUpdates,
        contains(new CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher(counterValue)));

    // Now let it finish and clean up
    processBundleLatch.countDown();
    MoreFutures.get(doneFuture);
  }

  /** Tests that successive metric updates overwrite the previous. */
  @Test(timeout = ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS * 10)
  public void testTentativeUserMetricsOverwrite() throws Exception {
    Supplier<String> idGenerator = makeIdGeneratorStartingFrom(777L);

    final String stepName = "fakeStepNameWithUserMetrics";
    final String namespace = "sdk/whatever";
    final String name = "someCounter";
    final int firstCounterValue = 42;
    final int secondCounterValue = 77;
    final CountDownLatch progressSentTwiceLatch = new CountDownLatch(2);
    final CountDownLatch processBundleLatch = new CountDownLatch(1);

    final BeamFnApi.Metrics.User.MetricName metricName =
        BeamFnApi.Metrics.User.MetricName.newBuilder()
            .setNamespace(namespace)
            .setName(name)
            .build();

    InstructionRequestHandler instructionRequestHandler =
        new InstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      processBundleLatch.await();
                      return responseFor(request)
                          .setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance())
                          .build();
                    });
              case PROCESS_BUNDLE_PROGRESS:
                progressSentTwiceLatch.countDown();
                return CompletableFuture.completedFuture(
                    responseFor(request)
                        .setProcessBundleProgress(
                            BeamFnApi.ProcessBundleProgressResponse.newBuilder()
                                .setMetrics(
                                    BeamFnApi.Metrics.newBuilder()
                                        .putPtransforms(GRPC_READ_ID, FAKE_ELEMENT_COUNT_METRICS)
                                        .putPtransforms(
                                            stepName,
                                            BeamFnApi.Metrics.PTransform.newBuilder()
                                                .addUser(
                                                    BeamFnApi.Metrics.User.newBuilder()
                                                        .setMetricName(metricName)
                                                        .setCounterData(
                                                            BeamFnApi.Metrics.User.CounterData
                                                                .newBuilder()
                                                                .setValue(
                                                                    progressSentTwiceLatch
                                                                                .getCount()
                                                                            > 0
                                                                        ? firstCounterValue
                                                                        : secondCounterValue)))
                                                .build())))
                        .build());
              default:
                // block forever
                return new CompletableFuture<>();
            }
          }

          @Override
          public void close() {}
        };

    when(grpcPortWriteOperation.processedElementsConsumer()).thenReturn(elementsConsumed -> {});

    RegisterAndProcessBundleOperation processOperation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            mockContext);

    BeamFnMapTaskExecutor mapTaskExecutor =
        BeamFnMapTaskExecutor.forOperations(
            ImmutableList.of(readOperation, grpcPortWriteOperation, processOperation),
            executionStateTracker);

    // Launch the BeamFnMapTaskExecutor and wait until we are sure there has been one
    // tentative update
    CompletionStage<Void> doneFuture = MoreFutures.runAsync(mapTaskExecutor::execute);
    progressSentTwiceLatch.await();

    Iterable<CounterUpdate> metricsCounterUpdates = Collections.emptyList();
    while (Iterables.size(metricsCounterUpdates) == 0) {
      Thread.sleep(ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS);
      metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();
    }

    assertThat(
        metricsCounterUpdates,
        contains(new CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher(secondCounterValue)));

    // Now let it finish and clean up
    processBundleLatch.countDown();
    MoreFutures.get(doneFuture);
  }
  @Test(timeout = ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS * 10)
  public void testFinalUserMetricsDeprecated() throws Exception {
    Supplier<String> idGenerator = makeIdGeneratorStartingFrom(777L);

    final String stepName = "fakeStepNameWithUserMetrics";
    final String namespace = "sdk/whatever";
    final String name = "someCounter";
    final int counterValue = 42;
    final int finalCounterValue = 77;
    final CountDownLatch progressSentLatch = new CountDownLatch(1);
    final CountDownLatch processBundleLatch = new CountDownLatch(1);

    final BeamFnApi.Metrics.User.MetricName metricName =
        BeamFnApi.Metrics.User.MetricName.newBuilder()
            .setNamespace(namespace)
            .setName(name)
            .build();

    InstructionRequestHandler instructionRequestHandler =
        new InstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      processBundleLatch.await();
                      return responseFor(request)
                          .setProcessBundle(
                              BeamFnApi.ProcessBundleResponse.newBuilder()
                                  .setMetrics(
                                      BeamFnApi.Metrics.newBuilder()
                                          .putPtransforms(GRPC_READ_ID, FAKE_ELEMENT_COUNT_METRICS)
                                          .putPtransforms(
                                              stepName,
                                              BeamFnApi.Metrics.PTransform.newBuilder()
                                                  .addUser(
                                                      BeamFnApi.Metrics.User.newBuilder()
                                                          .setMetricName(metricName)
                                                          .setCounterData(
                                                              BeamFnApi.Metrics.User.CounterData
                                                                  .newBuilder()
                                                                  .setValue(finalCounterValue)))
                                                  .build())))
                          .build();
                    });
              case PROCESS_BUNDLE_PROGRESS:
                progressSentLatch.countDown();
                return CompletableFuture.completedFuture(
                    responseFor(request)
                        .setProcessBundleProgress(
                            BeamFnApi.ProcessBundleProgressResponse.newBuilder()
                                .setMetrics(
                                    BeamFnApi.Metrics.newBuilder()
                                        .putPtransforms(GRPC_READ_ID, FAKE_ELEMENT_COUNT_METRICS)
                                        .putPtransforms(
                                            stepName,
                                            BeamFnApi.Metrics.PTransform.newBuilder()
                                                .addUser(
                                                    BeamFnApi.Metrics.User.newBuilder()
                                                        .setMetricName(metricName)
                                                        .setCounterData(
                                                            BeamFnApi.Metrics.User.CounterData
                                                                .newBuilder()
                                                                .setValue(counterValue)))
                                                .build())))
                        .build());
              default:
                // block forever
                return new CompletableFuture<>();
            }
          }

          @Override
          public void close() {}
        };

    RegisterAndProcessBundleOperation processOperation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            mockContext);

    BeamFnMapTaskExecutor mapTaskExecutor =
        BeamFnMapTaskExecutor.forOperations(
            ImmutableList.of(readOperation, grpcPortWriteOperation, processOperation),
            executionStateTracker);

    // Launch the BeamFnMapTaskExecutor and wait until we are sure there has been one
    // tentative update
    CompletionStage<Void> doneFuture = MoreFutures.runAsync(mapTaskExecutor::execute);
    progressSentLatch.await();

    // TODO: add ability to wait for tentative progress update
    Iterable<CounterUpdate> metricsCounterUpdates = Collections.emptyList();
    while (Iterables.size(metricsCounterUpdates) == 0) {
      Thread.sleep(ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS);
      metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();
    }

    // Get the final metrics
    processBundleLatch.countDown();
    MoreFutures.get(doneFuture);
    metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();

    assertThat(Iterables.size(metricsCounterUpdates), equalTo(1));

    assertThat(
        metricsCounterUpdates,
        contains(new CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher(finalCounterValue)));
  }

  @Test(timeout = ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS * 10)
  public void testExtractCounterUpdatesReturnsValidProgressTrackerCounterUpdatesIfPresent()
      throws Exception {

    Supplier<String> idGenerator = makeIdGeneratorStartingFrom(777L);

    final String stepName = "fakeStepNameWithUserMetrics";
    final String namespace = "sdk/whatever";
    final String name = "someCounter";
    final int counterValue = 42;
    final int finalCounterValue = 77;
    final CountDownLatch progressSentLatch = new CountDownLatch(1);
    final CountDownLatch processBundleLatch = new CountDownLatch(1);

    final BeamFnApi.Metrics.User.MetricName metricName =
        BeamFnApi.Metrics.User.MetricName.newBuilder()
            .setNamespace(namespace)
            .setName(name)
            .build();

    final BeamFnApi.Metrics deprecatedMetrics = BeamFnApi.Metrics.newBuilder()
        .putPtransforms(GRPC_READ_ID, FAKE_ELEMENT_COUNT_METRICS)
        .putPtransforms(
            stepName,
            BeamFnApi.Metrics.PTransform.newBuilder()
                .addUser(
                    BeamFnApi.Metrics.User.newBuilder()
                        .setMetricName(metricName)
                        .setCounterData(
                            BeamFnApi.Metrics.User.CounterData
                                .newBuilder()
                                .setValue(finalCounterValue)))
                .build()).build();

    final int expectedCounterValue = 5;
    final BeamFnApi.MonitoringInfo expectedMonitoringInfo = BeamFnApi.MonitoringInfo.newBuilder()
        .setUrn("beam:metric:user:ExpectedCounter")
        .setType("beam:metrics:sum_int_64")
        .putLabels("PTRANSFORM", "ExpectedPTransform")//todomigryz get proper ptransform name here
        .setMetric(BeamFnApi.Metric.newBuilder()
            .setCounterData(
                BeamFnApi.CounterData.newBuilder().setInt64Value(expectedCounterValue).build())
            .build())
        .build();

    InstructionRequestHandler instructionRequestHandler =
        new InstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      processBundleLatch.await();
                      return responseFor(request)
                          .setProcessBundle(
                              BeamFnApi.ProcessBundleResponse.newBuilder()
                                  .setMetrics(deprecatedMetrics)
                                  .addMonitoringInfos(expectedMonitoringInfo))
                              .build();
                    });
              case PROCESS_BUNDLE_PROGRESS:
                progressSentLatch.countDown();
                return CompletableFuture.completedFuture(
                    responseFor(request)
                        .setProcessBundleProgress(
                            BeamFnApi.ProcessBundleProgressResponse.newBuilder()
                                .setMetrics(deprecatedMetrics)
                                .addMonitoringInfos(expectedMonitoringInfo))
                        .build());
              default:
                throw new RuntimeException("Reached unexpected code path");
            }
          }

          @Override
          public void close() {}
        };

    RegisterAndProcessBundleOperation processOperation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            mockContext);

    BeamFnMapTaskExecutor mapTaskExecutor =
        BeamFnMapTaskExecutor.forOperations(
            ImmutableList.of(readOperation, grpcPortWriteOperation, processOperation),
            executionStateTracker);

    // Launch the BeamFnMapTaskExecutor and wait until we are sure there has been one
    // tentative update
    CompletionStage<Void> doneFuture = MoreFutures.runAsync(mapTaskExecutor::execute);
    progressSentLatch.await();

    // TODO: add ability to wait for tentative progress update
    Iterable<CounterUpdate> metricsCounterUpdates = Collections.emptyList();
    while (Iterables.size(metricsCounterUpdates) == 0) {
      Thread.sleep(ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS);
      metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();
    }

    // Get the final metrics
    processBundleLatch.countDown();
    MoreFutures.get(doneFuture);
    metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();

    assertThat(Iterables.size(metricsCounterUpdates), equalTo(1));

    assertThat(
        metricsCounterUpdates,
        contains(new CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher(expectedCounterValue)));
  }

  private Supplier<String> makeIdGeneratorStartingFrom(long initialValue) {
    AtomicLong longIdGenerator = new AtomicLong(initialValue);
    return () -> Long.toString(longIdGenerator.getAndIncrement());
  }

  private BeamFnApi.InstructionResponse.Builder responseFor(BeamFnApi.InstructionRequest request) {
    return BeamFnApi.InstructionResponse.newBuilder().setInstructionId(request.getInstructionId());
  }
}
