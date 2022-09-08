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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsEnvironment.MetricsEnvironmentState;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/** Tests for {@link PCollectionConsumerRegistryTest}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class PCollectionConsumerRegistryTest {

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
    sampler.stop();
  }

  @Test
  public void singleConsumer() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);

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

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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
    MetricsEnvironmentState metricsEnvironmentState = mock(MetricsEnvironmentState.class);

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            metricsEnvironmentState,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(P_COLLECTION_A, "pTransformA", "pTransformAName", consumerA1);
    consumers.register(P_COLLECTION_A, "pTransformB", "pTransformBName", consumerA2);

    // Test both cases; when there is an existing container and where there is no container
    MetricsContainer oldContainer = mock(MetricsContainer.class);
    when(metricsEnvironmentState.activate(metricsContainerRegistry.getContainer("pTransformA")))
        .thenReturn(oldContainer);
    when(metricsEnvironmentState.activate(metricsContainerRegistry.getContainer("pTransformB")))
        .thenReturn(null);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(P_COLLECTION_A);

    WindowedValue<String> element = valueInGlobalWindow("elem");
    wrapperConsumer.accept(element);

    // Verify that metrics environment state is updated with pTransformA's container, then reset to
    // the oldContainer, then pTransformB's container and then reset to null.
    InOrder inOrder = Mockito.inOrder(metricsEnvironmentState);
    inOrder
        .verify(metricsEnvironmentState)
        .activate(metricsContainerRegistry.getContainer("pTransformA"));
    inOrder.verify(metricsEnvironmentState).activate(oldContainer);
    inOrder
        .verify(metricsEnvironmentState)
        .activate(metricsContainerRegistry.getContainer("pTransformB"));
    inOrder.verify(metricsEnvironmentState).activate(null);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void testHandlesSplitsPassedToOriginalConsumer() throws Exception {
    final String pTransformIdA = "pTransformIdA";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporterAndRegistrar = new BundleProgressReporter.InMemory();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry,
            MetricsEnvironment::setCurrentContainer,
            sampler.create(),
            shortIds,
            reporterAndRegistrar,
            TEST_DESCRIPTOR);
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
