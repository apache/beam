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

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortWriteOperation;
import org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
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

  @Test(timeout = ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS * 60)
  public void testExtractCounterUpdatesReturnsValidProgressTrackerCounterUpdatesIfPresent()
      throws Exception {
    final CountDownLatch progressSentLatch = new CountDownLatch(1);
    final CountDownLatch processBundleLatch = new CountDownLatch(1);
    final int expectedCounterValue = 5;
    final MonitoringInfo expectedMonitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
            .putLabels(MonitoringInfoConstants.Labels.NAME, "ExpectedCounter")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "anyString")
            .setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "ExpectedPTransform")
            .setPayload(encodeInt64Counter(expectedCounterValue))
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
                                  .addMonitoringInfos(expectedMonitoringInfo))
                          .build();
                    });
              case PROCESS_BUNDLE_PROGRESS:
                progressSentLatch.countDown();
                return CompletableFuture.completedFuture(
                    responseFor(request)
                        .setProcessBundleProgress(
                            BeamFnApi.ProcessBundleProgressResponse.newBuilder()
                                .addMonitoringInfos(expectedMonitoringInfo))
                        .build());
              default:
                throw new RuntimeException("Reached unexpected code path");
            }
          }

          @Override
          public void registerProcessBundleDescriptor(ProcessBundleDescriptor descriptor) {}

          @Override
          public void close() {}
        };

    Map<String, DataflowStepContext> stepContextMap = new HashMap<>();
    stepContextMap.put("ExpectedPTransform", generateDataflowStepContext("Expected"));

    RegisterAndProcessBundleOperation processOperation =
        new RegisterAndProcessBundleOperation(
            IdGenerators.decrementingLongs(),
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            stepContextMap,
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
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

    // Get the final metrics
    processBundleLatch.countDown();
    MoreFutures.get(doneFuture);
    metricsCounterUpdates = mapTaskExecutor.extractMetricUpdates();

    assertThat(Iterables.size(metricsCounterUpdates), equalTo(1));
    CounterUpdate resultCounter = metricsCounterUpdates.iterator().next();

    assertTrue(
        new CounterHamcrestMatchers.CounterUpdateIntegerValueMatcher(expectedCounterValue)
            .matches(resultCounter));
    assertEquals(
        "ExpectedCounter", resultCounter.getStructuredNameAndMetadata().getName().getName());
  }

  /**
   * Generates bare minumum DataflowStepContext to use for testing.
   *
   * @param valuesPrefix prefix for all types of names that are specified in DataflowStepContext.
   * @return new instance of DataflowStepContext
   */
  private DataflowStepContext generateDataflowStepContext(String valuesPrefix) {
    NameContext nc =
        new NameContext() {
          @Nullable
          @Override
          public String stageName() {
            return valuesPrefix + "Stage";
          }

          @Nullable
          @Override
          public String originalName() {
            return valuesPrefix + "OriginalName";
          }

          @Nullable
          @Override
          public String systemName() {
            return valuesPrefix + "SystemName";
          }

          @Nullable
          @Override
          public String userName() {
            return valuesPrefix + "UserName";
          }
        };
    DataflowStepContext dsc =
        new DataflowStepContext(nc) {
          @Nullable
          @Override
          public <W extends BoundedWindow> TimerData getNextFiredTimer(Coder<W> windowCoder) {
            return null;
          }

          @Override
          public <W extends BoundedWindow> void setStateCleanupTimer(
              String timerId,
              W window,
              Coder<W> windowCoder,
              Instant cleanupTime,
              Instant cleanupOutputTimestamp) {}

          @Override
          public DataflowStepContext namespacedToUser() {
            return this;
          }

          @Override
          public StateInternals stateInternals() {
            return null;
          }

          @Override
          public TimerInternals timerInternals() {
            return null;
          }
        };
    return dsc;
  }

  private BeamFnApi.InstructionResponse.Builder responseFor(BeamFnApi.InstructionRequest request) {
    BeamFnApi.InstructionResponse.Builder response =
        BeamFnApi.InstructionResponse.newBuilder().setInstructionId(request.getInstructionId());
    if (request.hasRegister()) {
      response.setRegister(BeamFnApi.RegisterResponse.getDefaultInstance());
    } else if (request.hasProcessBundle()) {
      response.setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance());
    } else if (request.hasFinalizeBundle()) {
      response.setFinalizeBundle(BeamFnApi.FinalizeBundleResponse.getDefaultInstance());
    } else if (request.hasProcessBundleProgress()) {
      response.setProcessBundleProgress(
          BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    } else if (request.hasProcessBundleSplit()) {
      response.setProcessBundleSplit(BeamFnApi.ProcessBundleSplitResponse.getDefaultInstance());
    }
    return response;
  }
}
