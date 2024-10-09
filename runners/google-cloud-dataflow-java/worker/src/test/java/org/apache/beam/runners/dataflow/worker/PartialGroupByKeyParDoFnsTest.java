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

import static org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver.TestOutputCounter.getMeanByteCounterName;
import static org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver.TestOutputCounter.getObjectCounterName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder;
import org.apache.beam.runners.dataflow.worker.PartialGroupByKeyParDoFns.BatchSideInputPGBKParDoFn;
import org.apache.beam.runners.dataflow.worker.PartialGroupByKeyParDoFns.CoderSizeEstimator;
import org.apache.beam.runners.dataflow.worker.PartialGroupByKeyParDoFns.PairInfo;
import org.apache.beam.runners.dataflow.worker.PartialGroupByKeyParDoFns.StreamingSideInputPGBKParDoFn;
import org.apache.beam.runners.dataflow.worker.PartialGroupByKeyParDoFns.WindowingCoderGroupingKeyCreator;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.LongCounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingTables;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingTables.Combiner;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.SimplePartialGroupByKeyParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link PartialGroupByKeyParDoFns}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
})
public class PartialGroupByKeyParDoFnsTest {
  @Mock private StreamingSideInputFetcher<KV<String, Integer>, BoundedWindow> mockSideInputFetcher;
  @Mock private BagState<WindowedValue<KV<String, Integer>>> elemsBag;
  @Mock private SideInputReader mockSideInputReader;
  @Mock private StreamingModeExecutionContext.StepContext mockStreamingStepContext;
  @Mock private StateInternals mockStateInternals;
  @Mock private ValueState mockState;
  @Mock private Coder<String> mockCoder;

  private final CounterSet counterSet = new CounterSet();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPartialGroupByKey() throws Exception {
    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(
                WindowedValue.getValueOnlyCoder(
                    KvCoder.of(keyCoder, IterableCoder.of(valueCoder)))),
            counterSet,
            NameContextsForTests.nameContextForTest());

    ParDoFn pgbkParDoFn =
        new SimplePartialGroupByKeyParDoFn(
            GroupingTables.buffering(
                new WindowingCoderGroupingKeyCreator(keyCoder),
                PairInfo.create(),
                new CoderSizeEstimator(WindowedValue.getValueOnlyCoder(keyCoder)),
                new CoderSizeEstimator(valueCoder)),
            receiver);

    pgbkParDoFn.startBundle(receiver);

    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 4)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("there", 5)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 6)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("joe", 7)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("there", 8)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 9)));

    pgbkParDoFn.finishBundle();

    assertThat(
        receiver.outputElems,
        IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("hi", Arrays.asList(4, 6, 9))),
            WindowedValue.valueInGlobalWindow(KV.of("there", Arrays.asList(5, 8))),
            WindowedValue.valueInGlobalWindow(KV.of("joe", Arrays.asList(7)))));

    // Exact counter values depend on size of encoded data.  If encoding
    // changes, then these expected counters should change to match.
    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(getObjectCounterName("test_receiver_out"), false, 3L);
    verify(updateExtractor)
        .longMean(
            getMeanByteCounterName("test_receiver_out"),
            false,
            LongCounterMean.ZERO.addValue(49L, 3));
    verifyNoMoreInteractions(updateExtractor);
  }

  @Test
  public void testPartialGroupByKeyWithCombiner() throws Exception {
    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(
                WindowedValue.getValueOnlyCoder(KvCoder.of(keyCoder, valueCoder))),
            counterSet,
            NameContextsForTests.nameContextForTest());

    Combiner<WindowedValue<String>, Integer, Integer, Integer> combineFn = new TestCombiner();

    ParDoFn pgbkParDoFn =
        new SimplePartialGroupByKeyParDoFn(
            GroupingTables.combining(
                new WindowingCoderGroupingKeyCreator(keyCoder),
                PairInfo.create(),
                combineFn,
                new CoderSizeEstimator(WindowedValue.getValueOnlyCoder(keyCoder)),
                new CoderSizeEstimator(valueCoder)),
            receiver);

    pgbkParDoFn.startBundle(receiver);

    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 4)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("there", 5)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 6)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("joe", 7)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("there", 8)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 9)));

    pgbkParDoFn.finishBundle();

    assertThat(
        receiver.outputElems,
        IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("hi", 19)),
            WindowedValue.valueInGlobalWindow(KV.of("there", 13)),
            WindowedValue.valueInGlobalWindow(KV.of("joe", 7))));

    // Exact counter values depend on size of encoded data.  If encoding
    // changes, then these expected counters should change to match.
    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(getObjectCounterName("test_receiver_out"), false, 3L);
    verify(updateExtractor)
        .longMean(
            getMeanByteCounterName("test_receiver_out"),
            false,
            LongCounterMean.ZERO.addValue(25L, 3));
    verifyNoMoreInteractions(updateExtractor);
  }

  @Test
  public void testPartialGroupByKeyWithCombinerAndSideInputs() throws Exception {
    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(
                WindowedValue.getValueOnlyCoder(KvCoder.of(keyCoder, valueCoder))),
            counterSet,
            NameContextsForTests.nameContextForTest());

    Combiner<WindowedValue<String>, Integer, Integer, Integer> combineFn = new TestCombiner();

    ParDoFn pgbkParDoFn =
        new StreamingSideInputPGBKParDoFn(
            GroupingTables.combining(
                new WindowingCoderGroupingKeyCreator(keyCoder),
                PairInfo.create(),
                combineFn,
                new CoderSizeEstimator(WindowedValue.getValueOnlyCoder(keyCoder)),
                new CoderSizeEstimator(valueCoder)),
            receiver,
            mockSideInputFetcher);

    Set<BoundedWindow> readyWindows = ImmutableSet.<BoundedWindow>of(GlobalWindow.INSTANCE);
    when(mockSideInputFetcher.getReadyWindows()).thenReturn(readyWindows);
    when(mockSideInputFetcher.prefetchElements(readyWindows))
        .thenReturn(ImmutableList.of(elemsBag));
    when(elemsBag.read())
        .thenReturn(
            ImmutableList.of(
                WindowedValue.valueInGlobalWindow(KV.of("hi", 4)),
                WindowedValue.valueInGlobalWindow(KV.of("there", 5))));
    when(mockSideInputFetcher.storeIfBlocked(Matchers.<WindowedValue<KV<String, Integer>>>any()))
        .thenReturn(false, false, false, true);

    pgbkParDoFn.startBundle(receiver);

    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 6)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("joe", 7)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("there", 8)));
    pgbkParDoFn.processElement(WindowedValue.valueInGlobalWindow(KV.of("hi", 9)));

    pgbkParDoFn.finishBundle();

    assertThat(
        receiver.outputElems,
        IsIterableContainingInAnyOrder.<Object>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("hi", 10)),
            WindowedValue.valueInGlobalWindow(KV.of("there", 13)),
            WindowedValue.valueInGlobalWindow(KV.of("joe", 7))));

    // Exact counter values depend on size of encoded data.  If encoding
    // changes, then these expected counters should change to match.
    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(getObjectCounterName("test_receiver_out"), false, 3L);
    verify(updateExtractor)
        .longMean(
            getMeanByteCounterName("test_receiver_out"),
            false,
            LongCounterMean.ZERO.addValue(25L, 3));
    verifyNoMoreInteractions(updateExtractor);
  }

  @Test
  public void testCreateWithCombinerAndBatchSideInputs() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();
    KvCoder<String, Integer> kvCoder = KvCoder.of(keyCoder, valueCoder);

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(WindowedValue.getValueOnlyCoder(kvCoder)),
            counterSet,
            NameContextsForTests.nameContextForTest());

    StepContext stepContext =
        BatchModeExecutionContext.forTesting(options, "testStage")
            .getStepContext(TestOperationContext.create(counterSet));

    when(mockSideInputReader.isEmpty()).thenReturn(false);

    ParDoFn pgbk =
        PartialGroupByKeyParDoFns.create(
            options,
            kvCoder,
            AppliedCombineFn.withInputCoder(
                Sum.ofIntegers(),
                CoderRegistry.createDefault(ull),
                kvCoder,
                ImmutableList.<PCollectionView<?>>of(),
                WindowingStrategy.globalDefault()),
            mockSideInputReader,
            receiver,
            stepContext);
    assertTrue(pgbk instanceof BatchSideInputPGBKParDoFn);
  }

  @Test
  public void testCreateWithCombinerAndStreaming() throws Exception {
    StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
    options.setStreaming(true);

    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();
    KvCoder<String, Integer> kvCoder = KvCoder.of(keyCoder, valueCoder);

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(WindowedValue.getValueOnlyCoder(kvCoder)),
            counterSet,
            NameContextsForTests.nameContextForTest());

    ParDoFn pgbk =
        PartialGroupByKeyParDoFns.create(
            options,
            kvCoder,
            AppliedCombineFn.withInputCoder(
                Sum.ofIntegers(), CoderRegistry.createDefault(null), kvCoder),
            NullSideInputReader.empty(),
            receiver,
            null);
    assertTrue(pgbk instanceof SimplePartialGroupByKeyParDoFn);
  }

  @Test
  public void testCreateWithCombinerAndStreamingSideInputs() throws Exception {
    StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
    options.setStreaming(true);

    Coder keyCoder = StringUtf8Coder.of();
    Coder valueCoder = BigEndianIntegerCoder.of();
    KvCoder<String, Integer> kvCoder = KvCoder.of(keyCoder, valueCoder);

    TestOutputReceiver receiver =
        new TestOutputReceiver(
            new ElementByteSizeObservableCoder(WindowedValue.getValueOnlyCoder(kvCoder)),
            counterSet,
            NameContextsForTests.nameContextForTest());

    when(mockSideInputReader.isEmpty()).thenReturn(false);
    when(mockStreamingStepContext.stateInternals()).thenReturn((StateInternals) mockStateInternals);
    when(mockStateInternals.state(Matchers.<StateNamespace>any(), Matchers.<StateTag>any()))
        .thenReturn(mockState);
    when(mockState.read()).thenReturn(Maps.newHashMap());

    ParDoFn pgbk =
        PartialGroupByKeyParDoFns.create(
            options,
            kvCoder,
            AppliedCombineFn.withInputCoder(
                Sum.ofIntegers(),
                CoderRegistry.createDefault(null),
                kvCoder,
                ImmutableList.<PCollectionView<?>>of(),
                WindowingStrategy.globalDefault()),
            mockSideInputReader,
            receiver,
            mockStreamingStepContext);
    assertTrue(pgbk instanceof StreamingSideInputPGBKParDoFn);
  }

  /**
   * Test that CoderSizeEstimator correctly uses ElementByteSizeObserver if it reports itself as
   * non-lazy
   */
  @Test
  public void testCoderSizeEstimationWithNonLazyObserver() throws Exception {
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              ((ElementByteSizeObserver) args[1]).update(5);
              // Observer assumed non-lazy by default if setLazy not called
              return null;
            })
        .when(mockCoder)
        .registerByteSizeObserver(Matchers.eq("apple"), Matchers.<ElementByteSizeObserver>any());
    CoderSizeEstimator<String> estimator = new CoderSizeEstimator(mockCoder);
    assertEquals(5, estimator.estimateSize("apple"));
  }

  @Test
  public void testCoderSizeEstimationWithLazyObserver() throws Exception {
    // Have the code report that byte size observation is lazy
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              // Set lazy to true and do not update the size at all
              ((ElementByteSizeObserver) args[1]).setLazy();
              return null;
            })
        .when(mockCoder)
        .registerByteSizeObserver(Matchers.eq("apple"), Matchers.<ElementByteSizeObserver>any());

    // Encode the input to the output stream
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              String value = (String) args[0];
              OutputStream os = (OutputStream) args[1];
              os.write(value.getBytes(StandardCharsets.UTF_8));
              return null;
            })
        .when(mockCoder)
        .encode(Matchers.eq("apple"), Matchers.<OutputStream>any());
    CoderSizeEstimator<String> estimator = new CoderSizeEstimator(mockCoder);
    // Observer never updates size, so if result is 5, must have delegated to actual encoding
    assertEquals(5L, estimator.estimateSize("apple"));
  }

  private static class TestCombiner
      implements Combiner<WindowedValue<String>, Integer, Integer, Integer> {

    @Override
    public Integer createAccumulator(WindowedValue<String> key) {
      return 0;
    }

    @Override
    public Integer add(WindowedValue<String> key, Integer accumulator, Integer value) {
      return accumulator + value;
    }

    @Override
    public Integer merge(WindowedValue<String> key, Iterable<Integer> accumulators) {
      Integer sum = 0;
      for (Integer part : accumulators) {
        sum += part;
      }
      return sum;
    }

    @Override
    public Integer compact(WindowedValue<String> key, Integer accumulator) {
      return accumulator;
    }

    @Override
    public Integer extract(WindowedValue<String> key, Integer accumulator) {
      return accumulator;
    }
  }
}
