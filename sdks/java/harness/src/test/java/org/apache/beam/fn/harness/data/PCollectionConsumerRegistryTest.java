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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.debug.DataSampler;
import org.apache.beam.fn.harness.logging.BeamFnLoggingClient;
import org.apache.beam.fn.harness.logging.BeamFnLoggingMDC;
import org.apache.beam.fn.harness.logging.LoggingClientFactory;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

/** Tests for {@link PCollectionConsumerRegistryTest}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class PCollectionConsumerRegistryTest {
  private static final Counter TEST_USER_COUNTER = Metrics.counter("foo", "bar");

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static final String P_COLLECTION_A = "pCollectionA";
  private static final String P_COLLECTION_B = "pCollectionB";
  private static final ProcessBundleDescriptor TEST_DESCRIPTOR;

  static {
    SdkComponents sdkComponents = SdkComponents.create();
    try {
      String utf8CoderId = sdkComponents.registerCoder(StringUtf8Coder.of());
      String iterableUtf8CoderId =
          sdkComponents.registerCoder(IterableCoder.of(StringUtf8Coder.of()));

      TEST_DESCRIPTOR =
          ProcessBundleDescriptor.newBuilder()
              .putPcollections(
                  P_COLLECTION_A, PCollection.newBuilder().setCoderId(utf8CoderId).build())
              .putPcollections(
                  P_COLLECTION_B, PCollection.newBuilder().setCoderId(iterableUtf8CoderId).build())
              .putAllCoders(sdkComponents.toComponents().getCodersMap())
              .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ExecutionStateSampler sampler;

  @Before
  public void setUp() throws Exception {
    sampler = new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis);
  }

  @After
  public void tearDown() throws Exception {
    MetricsEnvironment.setCurrentContainer(null);
    sampler.stop();
  }

  @Test
  public void singleConsumer() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformIdA, pTransformIdA + "Name", consumerA1);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);
    String elementValue = "elem";
    WindowedValue<String> element = valueInGlobalWindow(elementValue);
    int numElements = 10;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);

    List<MonitoringInfo> expected = new ArrayList<>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64SumValue(numElements);
    expected.add(builder.build());

    long elementByteSize = StringUtf8Coder.of().getEncodedElementByteSize(elementValue);
    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(Urns.SAMPLED_BYTE_SIZE);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64DistributionValue(
        DistributionData.create(
            numElements * elementByteSize, numElements, elementByteSize, elementByteSize));
    expected.add(builder.build());

    Map<String, ByteString> actualData = new HashMap<>();
    reporterAndRegistrar.updateFinalMonitoringData(actualData);

    // Clear the timestamp before comparison.
    Iterable<MonitoringInfo> result =
        Iterables.filter(
            shortIds.toMonitoringInfo(actualData),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));

    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void singleConsumerException() throws Exception {
    final String pTransformId = "pTransformId";
    final String message = "testException";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumer = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumer);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);
    doThrow(new Exception(message)).when(consumer).accept(any());

    expectedException.expectMessage(message);
    expectedException.expect(Exception.class);
    wrapperConsumer.accept(valueInGlobalWindow("elem"));
  }

  /** Test that the counter increments even when there are no consumers of the PCollection. */
  @Test
  public void noConsumers() throws Exception {
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);
    String elementValue = "elem";
    WindowedValue<String> element = valueInGlobalWindow(elementValue);
    int numElements = 10;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    List<MonitoringInfo> expected = new ArrayList<>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64SumValue(numElements);
    expected.add(builder.build());

    long elementByteSize = StringUtf8Coder.of().getEncodedElementByteSize(elementValue);
    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(Urns.SAMPLED_BYTE_SIZE);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64DistributionValue(
        DistributionData.create(
            numElements * elementByteSize, numElements, elementByteSize, elementByteSize));
    expected.add(builder.build());

    Map<String, ByteString> actualData = new HashMap<>();
    reporterAndRegistrar.updateFinalMonitoringData(actualData);

    // Clear the timestamp before comparison.
    Iterable<MonitoringInfo> result =
        Iterables.filter(
            shortIds.toMonitoringInfo(actualData),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));

    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that the counter increments only once when multiple consumers of same pCollection read the
   * same element.
   */
  @Test
  public void multipleConsumersSamePCollection() throws Exception {
    final String pTransformIdA = "pTransformIdA";
    final String pTransformIdB = "pTransformIdB";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformIdA, pTransformIdA + "Name", consumerA1);
    consumers.register(P_COLLECTION_A, pTransformIdB, pTransformIdB + "Name", consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);

    String elementValue = "elem";
    WindowedValue<String> element = valueInGlobalWindow(elementValue);
    int numElements = 10;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);
    verify(consumerA2, times(numElements)).accept(element);

    ArrayList<MonitoringInfo> expected = new ArrayList<>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64SumValue(numElements);
    expected.add(builder.build());

    long elementByteSize = StringUtf8Coder.of().getEncodedElementByteSize(elementValue);
    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(Urns.SAMPLED_BYTE_SIZE);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_A);
    builder.setInt64DistributionValue(
        DistributionData.create(
            numElements * elementByteSize, numElements, elementByteSize, elementByteSize));
    expected.add(builder.build());

    Map<String, ByteString> actualData = new HashMap<>();
    reporterAndRegistrar.updateFinalMonitoringData(actualData);

    // Clear the timestamp before comparison.
    Iterable<MonitoringInfo> result =
        Iterables.filter(
            shortIds.toMonitoringInfo(actualData),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));
    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void multipleConsumersSamePCollectionException() throws Exception {
    final String pTransformId = "pTransformId";
    final String message = "testException";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumerA1);
    consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);
    doThrow(new Exception(message)).when(consumerA2).accept(any());

    expectedException.expectMessage(message);
    expectedException.expect(Exception.class);
    wrapperConsumer.accept(valueInGlobalWindow("elem"));
  }

  @Test
  public void throwsOnRegisteringAfterMultiplexingConsumerWasInitialized() throws Exception {
    final String pTransformId = "pTransformId";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumerA1);
    consumers.getMultiplexingConsumer(P_COLLECTION_A);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("cannot be register()-d after");
    consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumerA2);
  }

  @Test
  public void testMetricContainerUpdatedUponAcceptingElement() throws Exception {
    ExecutionStateTracker executionStateTracker = sampler.create();
    MetricsEnvironment.setCurrentContainer(executionStateTracker.getMetricsContainer());
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    executionStateTracker.start("testBundle");
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            executionStateTracker, shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);

    consumers.register(
        P_COLLECTION_A, "pTransformA", "pTransformAName", (unused) -> TEST_USER_COUNTER.inc());
    consumers.register(
        P_COLLECTION_A, "pTransformB", "pTransformBName", (unused) -> TEST_USER_COUNTER.inc(2));

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);

    WindowedValue<String> element = valueInGlobalWindow("elem");
    wrapperConsumer.accept(element);
    TEST_USER_COUNTER.inc(3);

    // Verify that metrics environment state is updated with pTransform's counters including the
    // unbound container when outside the scope of the function
    assertEquals(
        1L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getContainer("pTransformA")
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());
    assertEquals(
        2L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getContainer("pTransformB")
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());
    assertEquals(
        3L,
        (long)
            executionStateTracker
                .getMetricsContainerRegistry()
                .getUnboundContainer()
                .getCounter(TEST_USER_COUNTER.getName())
                .getCumulative());
  }

  @Test
  public void testHandlesSplitsPassedToOriginalConsumer() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    SplittingReceiver consumerA1 = mock(SplittingReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformIdA, pTransformIdA + "Name", consumerA1);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);

    assertTrue(wrapperConsumer instanceof HandlesSplits);

    ((HandlesSplits) wrapperConsumer).getProgress();
    verify(consumerA1).getProgress();

    ((HandlesSplits) wrapperConsumer).trySplit(0.3);
    verify(consumerA1).trySplit(0.3);
  }

  @Test
  public void testLazyByteSizeEstimation() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<Iterable<String>>> consumerA1 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_B, pTransformIdA, pTransformIdA + "Name", consumerA1);

    FnDataReceiver<WindowedValue<Iterable<String>>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<Iterable<String>>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_B);
    String elementValue = "elem";
    long elementByteSize = StringUtf8Coder.of().getEncodedElementByteSize(elementValue);
    WindowedValue<Iterable<String>> element =
        valueInGlobalWindow(
            new TestElementByteSizeObservableIterable<>(
                Arrays.asList(elementValue, elementValue), elementByteSize));
    int numElements = 10;
    // Mock doing work on the iterable items
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  Object[] args = invocation.getArguments();
                  WindowedValue<Iterable<String>> arg = (WindowedValue<Iterable<String>>) args[0];
                  Iterator it = arg.getValue().iterator();
                  while (it.hasNext()) {
                    it.next();
                  }
                  return null;
                })
        .when(consumerA1)
        .accept(element);

    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);

    List<MonitoringInfo> expected = new ArrayList<>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_B);
    builder.setInt64SumValue(numElements);
    expected.add(builder.build());

    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(Urns.SAMPLED_BYTE_SIZE);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, P_COLLECTION_B);
    long expectedBytes =
        (elementByteSize + 1) * 2
            + 5; // Additional 5 bytes are due to size and hasNext = false (1 byte).
    builder.setInt64DistributionValue(
        DistributionData.create(
            numElements * expectedBytes, numElements, expectedBytes, expectedBytes));
    expected.add(builder.build());

    Map<String, ByteString> actualData = new HashMap<>();
    reporterAndRegistrar.updateFinalMonitoringData(actualData);

    // Clear the timestamp before comparison.
    Iterable<MonitoringInfo> result =
        Iterables.filter(
            shortIds.toMonitoringInfo(actualData),
            monitoringInfo -> monitoringInfo.containsLabels(Labels.PCOLLECTION));

    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that element samples are taken when a DataSampler is present.
   *
   * @throws Exception
   */
  @Test
  public void dataSampling() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    DataSampler dataSampler = new DataSampler();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporterAndRegistrar, TEST_DESCRIPTOR, dataSampler);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, pTransformIdA, pTransformIdA + "Name", consumerA1);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);
    String elementValue = "elem";
    WindowedValue<String> element = valueInGlobalWindow(elementValue);
    int numElements = 10;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSampleData(BeamFnApi.SampleDataRequest.newBuilder())
            .build();
    BeamFnApi.InstructionResponse response = dataSampler.handleDataSampleRequest(request).build();

    Map<String, BeamFnApi.SampleDataResponse.ElementList> elementSamplesMap =
        response.getSampleData().getElementSamplesMap();

    assertFalse(elementSamplesMap.isEmpty());

    BeamFnApi.SampleDataResponse.ElementList elementList = elementSamplesMap.get(P_COLLECTION_A);
    assertNotNull(elementList);

    List<BeamFnApi.SampledElement> expectedSamples = new ArrayList<>();
    StringUtf8Coder coder = StringUtf8Coder.of();
    for (int i = 0; i < numElements; i++) {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      coder.encode(elementValue, stream);
      expectedSamples.add(
          BeamFnApi.SampledElement.newBuilder().setElement(stream.toByteStringAndReset()).build());
    }

    assertTrue(elementList.getElementsList().containsAll(expectedSamples));
  }

  @Test
  public void logsExceptionWithTransformId() throws Exception {
    final String pTransformId = "pTransformId";
    final String message = "testException";
    final String instructionId = "instruction";
    final Exception thrownException = new Exception(message);

    // The following is a bunch of boiler-plate to set up a local FnApiLoggingService to catch any
    // logs for later test
    // expectations.
    AtomicBoolean clientClosedStream = new AtomicBoolean();
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.LogEntry.List logEntries) ->
                    values.addAll(logEntries.getLogEntriesList()))
            .withOnCompleted(
                () -> {
                  // Remember that the client told us that this stream completed
                  clientClosedStream.set(true);
                  outboundServerObserver.get().onCompleted();
                })
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();
    ManagedChannel channel = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    // End logging boiler-plate...

    // This section is to set up the StateSampler with the expected metadata.
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis);
    ExecutionStateSampler.ExecutionStateTracker stateTracker = sampler.create();
    stateTracker.start("process-bundle");
    ExecutionStateSampler.ExecutionState state =
        stateTracker.create("shortId", pTransformId, pTransformId, "process");
    state.activate();

    // Track the instruction and state in the logging system. In a real run, this is set when a
    // ProcessBundlehandler
    // starts processing.
    BeamFnLoggingMDC.setInstructionId(instructionId);
    BeamFnLoggingMDC.setStateTracker(stateTracker);

    // Start the test within the logging context. This reroutes logging through to the boiler-plate
    // that was set up
    // earlier.
    try (BeamFnLoggingClient ignored =
        LoggingClientFactory.createAndStart(
            PipelineOptionsFactory.create(),
            apiServiceDescriptor,
            (Endpoints.ApiServiceDescriptor descriptor) -> channel)) {

      // Set up the component under test, the FnDataReceiver, to emit an exception when it starts.
      ShortIdMap shortIds = new ShortIdMap();
      BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
      PCollectionConsumerRegistry consumers =
          new PCollectionConsumerRegistry(
              stateTracker, shortIds, reporterAndRegistrar, TEST_DESCRIPTOR);
      FnDataReceiver<WindowedValue<String>> consumer = mock(FnDataReceiver.class);

      consumers.register(P_COLLECTION_A, pTransformId, pTransformId + "Name", consumer);

      FnDataReceiver<WindowedValue<String>> wrapperConsumer =
          (FnDataReceiver<WindowedValue<String>>)
              (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);

      doThrow(thrownException).when(consumer).accept(any());
      expectedException.expectMessage(message);
      expectedException.expect(Exception.class);

      // Run the test.
      wrapperConsumer.accept(valueInGlobalWindow("elem"));

    } finally {
      // The actual log entry has a lot of metadata that can't easily be controlled. So set the
      // entries that are needed
      // for this test and cull everything else.
      final BeamFnApi.LogEntry expectedEntry =
          BeamFnApi.LogEntry.newBuilder()
              .setInstructionId(instructionId)
              .setTransformId(pTransformId)
              .setMessage("Failed to process element for bundle \"process-bundle\"")
              .build();

      List<BeamFnApi.LogEntry> entries = new ArrayList<>(values);
      assertEquals(1, entries.size());
      BeamFnApi.LogEntry actualEntry = entries.get(0);
      BeamFnApi.LogEntry actualEntryCulled =
          BeamFnApi.LogEntry.newBuilder()
              .setInstructionId(actualEntry.getInstructionId())
              .setTransformId(actualEntry.getTransformId())
              .setMessage(actualEntry.getMessage())
              .build();

      assertEquals(expectedEntry, actualEntryCulled);

      server.shutdownNow();
    }
  }

  private static class TestElementByteSizeObservableIterable<T>
      extends ElementByteSizeObservableIterable<T, ElementByteSizeObservableIterator<T>> {
    private List<T> elements;
    private long elementByteSize;

    public TestElementByteSizeObservableIterable(List<T> elements, long elementByteSize) {
      this.elements = elements;
      this.elementByteSize = elementByteSize;
    }

    @Override
    protected ElementByteSizeObservableIterator createIterator() {
      return new ElementByteSizeObservableIterator() {
        private int index = 0;

        @Override
        public boolean hasNext() {
          return index < elements.size();
        }

        @Override
        public Object next() {
          notifyValueReturned(elementByteSize);
          return elements.get(index++);
        }
      };
    }
  }

  private abstract static class SplittingReceiver<T> implements FnDataReceiver<T>, HandlesSplits {}
}
