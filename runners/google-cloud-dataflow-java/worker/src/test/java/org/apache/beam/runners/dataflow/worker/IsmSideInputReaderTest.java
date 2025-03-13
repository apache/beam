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

import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.concat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SplitInt64;
import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink.SinkWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link IsmSideInputReader}.
 *
 * <p>Note that we use a {@link BigEndianLongCoder} to because their byte representation compares
 * equivalently to their numeric representation for non-negative values.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"keyfor"})
public class IsmSideInputReaderTest {

  private static final long BLOOM_FILTER_SIZE_LIMIT = 10_000;
  private static final int NUM_THREADS = 16;
  private static final DataflowPipelineOptions pipelineOptions =
      PipelineOptionsFactory.as(DataflowPipelineOptions.class);

  private static final CounterSet counterFactory = new CounterSet();
  private static final BatchModeExecutionContext executionContext =
      BatchModeExecutionContext.forTesting(
          pipelineOptions, counterFactory, NameContextsForTests.nameContextForTest().stageName());
  private static final DataflowOperationContext operationContext =
      executionContext.createOperationContext(NameContextsForTests.nameContextForTest());
  private Closer setupCloser;

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Coder<BoundedWindow> GLOBAL_WINDOW_CODER =
      (Coder) GlobalWindow.Coder.INSTANCE;

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Coder<BoundedWindow> INTERVAL_WINDOW_CODER =
      (Coder) IntervalWindow.getCoder();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    pipelineOptions.as(DataflowPipelineDebugOptions.class);
    setupCloser = Closer.create();
    setupCloser.register(executionContext.getExecutionStateTracker().activate());
    setupCloser.register(operationContext.enterProcess());
  }

  @After
  public void tearDown() throws IOException {
    setupCloser.close();
  }

  @Test
  public void testSingleton() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    final WindowedValue<Long> element = valueInGlobalWindow(42L);
    final PCollectionView<Long> view =
        Pipeline.create().apply(Create.empty(VarLongCoder.of())).apply(View.asSingleton());

    final Source source =
        initInputFile(
            fromValues(Arrays.asList(element)),
            IsmRecordCoder.of(1, 0, ImmutableList.<Coder<?>>of(GLOBAL_WINDOW_CODER), valueCoder));

    final IsmSideInputReader reader = sideInputReader(view.getTagInternal().getId(), source);

    List<Callable<Long>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Long value = reader.get(view, GlobalWindow.INSTANCE);
            assertEquals(element.getValue(), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(value, reader.get(view, GlobalWindow.INSTANCE));
            return value;
          });
    }

    List<Future<Long>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    // Assert that all threads got back the same reference
    Long value = results.get(0).get();
    for (Future<Long> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testSingletonInWindow() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(1, 0, ImmutableList.<Coder<?>>of(INTERVAL_WINDOW_CODER), valueCoder);

    final List<WindowedValue<Long>> elements =
        Arrays.asList(
            valueInIntervalWindow(12, 0),
            valueInIntervalWindow(17, 10),
            valueInIntervalWindow(28, 20));
    final Long defaultValue = 42L;

    final PCollectionView<Long> view =
        Pipeline.create()
            .apply(Create.empty(VarLongCoder.of()))
            .apply(Window.into(FixedWindows.of(Duration.millis(1))))
            .apply(View.<Long>asSingleton().withDefaultValue(defaultValue));

    Source sourceA = initInputFile(fromValues(elements).subList(0, 1), ismCoder);
    Source sourceB = initInputFile(fromValues(elements).subList(1, 3), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<Map<BoundedWindow, Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            Map<BoundedWindow, Long> rval = new HashMap<>();
            for (WindowedValue<Long> element : elements) {
              // Store a strong reference to the returned value so that the logical reference
              // cache is not cleared for this test.
              Long value = reader.get(view, windowOf(element));
              assertEquals(element.getValue(), value);
              // Assert that the same value reference was returned showing that it was cached.
              assertSame(value, reader.get(view, windowOf(element)));
              rval.put(windowOf(element), value);
            }
            // Check that if we don't find a value for a given window, we return the default.
            assertEquals(defaultValue, reader.get(view, intervalWindow(30)));
            return rval;
          });
    }

    List<Future<Map<BoundedWindow, Long>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, Long> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, Long>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Long> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testSingletonMap() throws Exception {
    final WindowedValue<Map<String, Long>> element =
        valueInGlobalWindow(
            ImmutableMap.<String, Long>builder().put("foo", 0L).put("bar", -1L).build());
    Coder<Map<String, Long>> mapCoder = MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
    final PCollectionView<Map<String, Long>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
            .apply(View.asMap());

    IsmRecordCoder<WindowedValue<Map<String, Long>>> recordCoder =
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.<Coder<?>>of(GLOBAL_WINDOW_CODER),
            WindowedValue.getFullCoder(mapCoder, GLOBAL_WINDOW_CODER));
    final Source source = initInputFile(fromValues(Arrays.asList(element)), recordCoder);

    final IsmSideInputReader reader = sideInputReader(view.getTagInternal().getId(), source);

    List<Callable<Map<String, Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<String, Long> value = reader.get(view, GlobalWindow.INSTANCE);
            assertEquals(element.getValue(), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(value, reader.get(view, GlobalWindow.INSTANCE));
            return value;
          });
    }

    List<Future<Map<String, Long>>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    // Assert that all threads got back the same reference
    Map<String, Long> value = results.get(0).get();
    for (Future<Map<String, Long>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testSingletonMapInWindow() throws Exception {
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0L), new Instant(100L));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(50L), new Instant(150L));
    IntervalWindow emptyWindow = new IntervalWindow(new Instant(75L), new Instant(175L));
    final Map<IntervalWindow, WindowedValue<Map<String, Long>>> elements =
        ImmutableMap.<IntervalWindow, WindowedValue<Map<String, Long>>>builder()
            .put(
                firstWindow,
                WindowedValue.of(
                    ImmutableMap.<String, Long>builder().put("foo", 0L).put("bar", -1L).build(),
                    new Instant(7),
                    firstWindow,
                    PaneInfo.NO_FIRING))
            .put(
                secondWindow,
                WindowedValue.of(
                    ImmutableMap.<String, Long>builder().put("bar", -1L).put("baz", 1L).build(),
                    new Instant(53L),
                    secondWindow,
                    PaneInfo.NO_FIRING))
            .build();
    Coder<Map<String, Long>> mapCoder = MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
    final PCollectionView<Map<String, Long>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
            .apply(
                Window.into(SlidingWindows.of(Duration.millis(100L)).every(Duration.millis(50L))))
            .apply(View.asMap());

    IsmRecordCoder<WindowedValue<Map<String, Long>>> recordCoder =
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.<Coder<?>>of(INTERVAL_WINDOW_CODER),
            WindowedValue.getFullCoder(mapCoder, INTERVAL_WINDOW_CODER));
    final Source source = initInputFile(fromValues(elements.values()), recordCoder);

    final IsmSideInputReader reader = sideInputReader(view.getTagInternal().getId(), source);

    List<Callable<Map<BoundedWindow, Map<String, Long>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<String, Long> value = reader.get(view, firstWindow);
            assertEquals(elements.get(firstWindow).getValue(), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(value, reader.get(view, firstWindow));
            Map<String, Long> secondValue = reader.get(view, secondWindow);
            assertEquals(elements.get(secondWindow).getValue(), secondValue);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(secondValue, reader.get(view, secondWindow));
            Map<String, Long> emptyValue = reader.get(view, emptyWindow);
            assertThat(emptyValue.keySet(), empty());
            Map<BoundedWindow, Map<String, Long>> result =
                ImmutableMap.<BoundedWindow, Map<String, Long>>builder()
                    .put(firstWindow, value)
                    .put(secondWindow, secondValue)
                    .put(emptyWindow, emptyValue)
                    .build();
            return result;
          });
    }

    List<Future<Map<BoundedWindow, Map<String, Long>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    // Assert that all threads got back the same reference
    Map<BoundedWindow, Map<String, Long>> value = results.get(0).get();
    for (Future<Map<BoundedWindow, Map<String, Long>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Map<String, Long>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testSingletonMultimapInWindow() throws Exception {
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0L), new Instant(100L));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(50L), new Instant(150L));
    IntervalWindow emptyWindow = new IntervalWindow(new Instant(75L), new Instant(175L));
    @SuppressWarnings({"unchecked", "rawtypes"}) // Collection is iterable, and this is immutable
    final Map<IntervalWindow, WindowedValue<Map<String, Iterable<Long>>>> elements =
        ImmutableMap.<IntervalWindow, WindowedValue<Map<String, Iterable<Long>>>>builder()
            .put(
                firstWindow,
                WindowedValue.of(
                    (Map)
                        ImmutableListMultimap.<String, Long>builder()
                            .put("foo", 0L)
                            .put("foo", 2L)
                            .put("bar", -1L)
                            .build()
                            .asMap(),
                    new Instant(7),
                    firstWindow,
                    PaneInfo.NO_FIRING))
            .put(
                secondWindow,
                WindowedValue.of(
                    (Map)
                        ImmutableListMultimap.<String, Long>builder()
                            .put("bar", -1L)
                            .put("baz", 1L)
                            .put("baz", 3L)
                            .build()
                            .asMap(),
                    new Instant(53L),
                    secondWindow,
                    PaneInfo.NO_FIRING))
            .build();
    StringUtf8Coder strCoder = StringUtf8Coder.of();
    Coder<Map<String, Iterable<Long>>> mapCoder =
        MapCoder.of(strCoder, IterableCoder.of(VarLongCoder.of()));
    final PCollectionView<Map<String, Iterable<Long>>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
            .apply(Window.into(FixedWindows.of(Duration.millis(100L))))
            .apply(View.asMultimap());

    IsmRecordCoder<WindowedValue<Map<String, Iterable<Long>>>> recordCoder =
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.<Coder<?>>of(INTERVAL_WINDOW_CODER),
            WindowedValue.getFullCoder(mapCoder, INTERVAL_WINDOW_CODER));
    final Source source = initInputFile(fromValues(elements.values()), recordCoder);

    final IsmSideInputReader reader = sideInputReader(view.getTagInternal().getId(), source);

    List<Callable<Map<BoundedWindow, Map<String, Iterable<Long>>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<String, Iterable<Long>> value = reader.get(view, firstWindow);
            assertEquals(elements.get(firstWindow).getValue(), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(value, reader.get(view, firstWindow));
            Map<String, Iterable<Long>> secondValue = reader.get(view, secondWindow);
            assertEquals(elements.get(secondWindow).getValue(), secondValue);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(secondValue, reader.get(view, secondWindow));
            Map<String, Iterable<Long>> emptyValue = reader.get(view, emptyWindow);
            assertThat(emptyValue.keySet(), empty());
            Map<BoundedWindow, Map<String, Iterable<Long>>> result =
                ImmutableMap.<BoundedWindow, Map<String, Iterable<Long>>>builder()
                    .put(firstWindow, value)
                    .put(secondWindow, secondValue)
                    .put(emptyWindow, emptyValue)
                    .build();
            return result;
          });
    }

    List<Future<Map<BoundedWindow, Map<String, Iterable<Long>>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, Map<String, Iterable<Long>>> value = results.get(0).get();
    for (Future<Map<BoundedWindow, Map<String, Iterable<Long>>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Map<String, Iterable<Long>>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testIterable() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(GLOBAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(12L)),
            KV.of(1L, valueInGlobalWindow(22L)),
            KV.of(2L, valueInGlobalWindow(32L)));
    final List<KV<Long, WindowedValue<Long>>> secondElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(42L)),
            KV.of(1L, valueInGlobalWindow(52L)),
            KV.of(2L, valueInGlobalWindow(62L)));

    final PCollectionView<Iterable<Long>> view =
        Pipeline.create().apply(Create.empty(VarLongCoder.of())).apply(View.asIterable());

    Source sourceA = initInputFile(fromKvsForList(firstElements), ismCoder);
    Source sourceB = initInputFile(fromKvsForList(secondElements), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<Iterable<Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Iterable<Long> value = reader.get(view, GlobalWindow.INSTANCE);
            verifyIterable(toValueList(concat(firstElements, secondElements)), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(reader.get(view, GlobalWindow.INSTANCE), value);
            return value;
          });
    }

    List<Future<Iterable<Long>>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    Iterable<Long> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Iterable<Long>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testIterableAtN() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(GLOBAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(12L)),
            KV.of(1L, valueInGlobalWindow(22L)),
            KV.of(2L, valueInGlobalWindow(32L)));
    final List<KV<Long, WindowedValue<Long>>> secondElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(42L)),
            KV.of(1L, valueInGlobalWindow(52L)),
            KV.of(2L, valueInGlobalWindow(62L)));

    final PCollectionView<Iterable<Long>> view =
        Pipeline.create().apply(Create.empty(VarLongCoder.of())).apply(View.asIterable());

    String tmpFilePrefix = tmpFolder.newFile().getPath();
    initInputFile(fromKvsForList(firstElements), ismCoder, tmpFilePrefix + "-00000-of-00002.ism");
    initInputFile(fromKvsForList(secondElements), ismCoder, tmpFilePrefix + "-00001-of-00002.ism");

    Source source = newIsmSource(ismCoder, tmpFilePrefix + "@2.ism");

    final IsmSideInputReader reader = sideInputReader(view.getTagInternal().getId(), source);

    List<Callable<Iterable<Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Iterable<Long> value = reader.get(view, GlobalWindow.INSTANCE);
            verifyIterable(toValueList(concat(firstElements, secondElements)), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(reader.get(view, GlobalWindow.INSTANCE), value);
            return value;
          });
    }

    List<Future<Iterable<Long>>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    Iterable<Long> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Iterable<Long>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testIterableInWindow() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(INTERVAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(12, 10)),
            KV.of(1L, valueInIntervalWindow(22, 10)),
            KV.of(2L, valueInIntervalWindow(32, 10)));
    final List<KV<Long, WindowedValue<Long>>> secondElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(42, 20)),
            KV.of(1L, valueInIntervalWindow(52, 20)),
            KV.of(2L, valueInIntervalWindow(62, 20)));
    final List<KV<Long, WindowedValue<Long>>> thirdElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(42L, 30)),
            KV.of(1L, valueInIntervalWindow(52L, 30)),
            KV.of(2L, valueInIntervalWindow(62L, 30)));

    final PCollectionView<Iterable<Long>> view =
        Pipeline.create()
            .apply(Create.empty(VarLongCoder.of()))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asIterable());

    Source sourceA = initInputFile(fromKvsForList(concat(firstElements, secondElements)), ismCoder);
    Source sourceB = initInputFile(fromKvsForList(thirdElements), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<Map<BoundedWindow, Iterable<Long>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Iterable<Long> firstValues = reader.get(view, intervalWindow(10));
            Iterable<Long> secondValues = reader.get(view, intervalWindow(20));
            Iterable<Long> thirdValues = reader.get(view, intervalWindow(30));

            verifyIterable(toValueList(firstElements), firstValues);
            verifyIterable(toValueList(secondElements), secondValues);
            verifyIterable(toValueList(thirdElements), thirdValues);

            // Assert that the same value reference was returned showing that it was cached.
            assertSame(firstValues, reader.get(view, intervalWindow(10)));
            assertSame(secondValues, reader.get(view, intervalWindow(20)));
            assertSame(thirdValues, reader.get(view, intervalWindow(30)));
            return ImmutableMap.<BoundedWindow, Iterable<Long>>of(
                intervalWindow(10), firstValues,
                intervalWindow(20), secondValues,
                intervalWindow(30), thirdValues);
          });
    }

    List<Future<Map<BoundedWindow, Iterable<Long>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, Iterable<Long>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, Iterable<Long>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Iterable<Long>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testList() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(GLOBAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(12L)),
            KV.of(1L, valueInGlobalWindow(22L)),
            KV.of(2L, valueInGlobalWindow(32L)));
    final List<KV<Long, WindowedValue<Long>>> secondElements =
        Arrays.asList(
            KV.of(0L, valueInGlobalWindow(42L)),
            KV.of(1L, valueInGlobalWindow(52L)),
            KV.of(2L, valueInGlobalWindow(62L)));

    final PCollectionView<List<Long>> view =
        Pipeline.create()
            .apply(Create.empty(VarLongCoder.of()))
            .apply(View.<Long>asList().withRandomAccess());

    Source sourceA = initInputFile(fromKvsForList(firstElements), ismCoder);
    Source sourceB = initInputFile(fromKvsForList(secondElements), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<List<Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            List<Long> value = reader.get(view, GlobalWindow.INSTANCE);
            verifyList(toValueList(concat(firstElements, secondElements)), value);
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(reader.get(view, GlobalWindow.INSTANCE), value);
            return value;
          });
    }

    List<Future<List<Long>>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    List<Long> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<List<Long>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testListInWindow() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(INTERVAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(12, 10)),
            KV.of(1L, valueInIntervalWindow(22, 10)),
            KV.of(2L, valueInIntervalWindow(32, 10)));
    final List<KV<Long, WindowedValue<Long>>> secondElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(42, 20)),
            KV.of(1L, valueInIntervalWindow(52, 20)),
            KV.of(2L, valueInIntervalWindow(62, 20)));
    final List<KV<Long, WindowedValue<Long>>> thirdElements =
        Arrays.asList(
            KV.of(0L, valueInIntervalWindow(42L, 30)),
            KV.of(1L, valueInIntervalWindow(52L, 30)),
            KV.of(2L, valueInIntervalWindow(62L, 30)));

    final PCollectionView<List<Long>> view =
        Pipeline.create()
            .apply(Create.empty(VarLongCoder.of()))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<Long>asList().withRandomAccess());

    Source sourceA = initInputFile(fromKvsForList(concat(firstElements, secondElements)), ismCoder);
    Source sourceB = initInputFile(fromKvsForList(thirdElements), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<Map<BoundedWindow, List<Long>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            List<Long> firstValues = reader.get(view, intervalWindow(10));
            List<Long> secondValues = reader.get(view, intervalWindow(20));
            List<Long> thirdValues = reader.get(view, intervalWindow(30));

            verifyList(toValueList(firstElements), firstValues);
            verifyList(toValueList(secondElements), secondValues);
            verifyList(toValueList(thirdElements), thirdValues);

            // Assert that the same value reference was returned showing that it was cached.
            assertSame(firstValues, reader.get(view, intervalWindow(10)));
            assertSame(secondValues, reader.get(view, intervalWindow(20)));
            assertSame(thirdValues, reader.get(view, intervalWindow(30)));

            // Also verify when requesting a window that is not part of the side input
            assertEquals(Collections.EMPTY_LIST, reader.get(view, intervalWindow(40)));

            return ImmutableMap.<BoundedWindow, List<Long>>of(
                intervalWindow(10), firstValues,
                intervalWindow(20), secondValues,
                intervalWindow(30), thirdValues);
          });
    }

    List<Future<Map<BoundedWindow, List<Long>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, List<Long>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, List<Long>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, List<Long>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testMap() throws Exception {
    // Note that we purposely use byte[]s as keys to force structural equality testing
    // versus using java equality testing.
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    final ListMultimap<byte[], WindowedValue<Long>> elements =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInGlobalWindow(12L))
            .put(new byte[] {0x01}, valueInGlobalWindow(22L))
            .put(new byte[] {0x02}, valueInGlobalWindow(32L))
            .put(new byte[] {0x03}, valueInGlobalWindow(42L))
            .put(new byte[] {0x04}, valueInGlobalWindow(52L))
            .put(new byte[] {0x05}, valueInGlobalWindow(62L))
            .build();

    final PCollectionView<Map<byte[], Long>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of())))
            .apply(View.asMap());

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(ByteArrayCoder.of()),
                GLOBAL_WINDOW_CODER,
                BigEndianLongCoder.of()),
            valueCoder);

    Multimap<Integer, IsmRecord<WindowedValue<Long>>> elementsPerShard = forMap(ismCoder, elements);
    List<IsmRecord<WindowedValue<Long>>> firstElements = new ArrayList<>();
    List<IsmRecord<WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (Map.Entry<Integer, Collection<IsmRecord<WindowedValue<Long>>>> entry :
        elementsPerShard.asMap().entrySet()) {
      if (entry.getKey() % 2 == 0) {
        firstElements.addAll(entry.getValue());
      } else {
        secondElements.addAll(entry.getValue());
      }
    }
    // Ensure that each file will have some records.
    checkState(!firstElements.isEmpty());
    checkState(!secondElements.isEmpty());

    Source sourceA = initInputFile(firstElements, ismCoder);
    Source sourceB = initInputFile(secondElements, ismCoder);

    List<IsmRecord<WindowedValue<Long>>> mapMetadata =
        forMapMetadata(ByteArrayCoder.of(), elements.keySet(), GlobalWindow.INSTANCE);
    Source sourceMeta = initInputFile(mapMetadata, ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB, sourceMeta);

    List<Callable<Map<byte[], Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<byte[], Long> value = reader.get(view, GlobalWindow.INSTANCE);
            verifyMap(
                Maps.transformValues(elements.asMap(), new TransformForMap<Long>()),
                value,
                new ComparatorForMap<Long>());
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(reader.get(view, GlobalWindow.INSTANCE), value);

            return value;
          });
    }

    List<Future<Map<byte[], Long>>> results = pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<byte[], Long> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<byte[], Long>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testMapInWindow() throws Exception {
    // Note that we purposely use byte[]s as keys to force structural equality testing
    // versus using java equality testing.
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    final ListMultimap<byte[], WindowedValue<Long>> firstWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(12L, 10))
            .put(new byte[] {0x01}, valueInIntervalWindow(22L, 10))
            .put(new byte[] {0x02}, valueInIntervalWindow(32L, 10))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> secondWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(42L, 20))
            .put(new byte[] {0x03}, valueInIntervalWindow(52L, 20))
            .put(new byte[] {0x02}, valueInIntervalWindow(62L, 20))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> thirdWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x02}, valueInIntervalWindow(72L, 30))
            .put(new byte[] {0x04}, valueInIntervalWindow(82L, 30))
            .put(new byte[] {0x05}, valueInIntervalWindow(92L, 30))
            .build();

    final PCollectionView<Map<byte[], Long>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of())))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMap());

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(ByteArrayCoder.of()),
                INTERVAL_WINDOW_CODER,
                BigEndianLongCoder.of()),
            valueCoder);

    Multimap<Integer, IsmRecord<WindowedValue<Long>>> elementsPerShard =
        forMap(ismCoder, firstWindow);
    elementsPerShard.putAll(forMap(ismCoder, secondWindow));
    elementsPerShard.putAll(forMap(ismCoder, thirdWindow));

    List<IsmRecord<WindowedValue<Long>>> firstElements = new ArrayList<>();
    List<IsmRecord<WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (Map.Entry<Integer, Collection<IsmRecord<WindowedValue<Long>>>> entry :
        elementsPerShard.asMap().entrySet()) {
      if (entry.getKey() % 2 == 0) {
        firstElements.addAll(entry.getValue());
      } else {
        secondElements.addAll(entry.getValue());
      }
    }
    // Ensure that each file will have some records.
    checkState(!firstElements.isEmpty());
    checkState(!secondElements.isEmpty());

    Source sourceA = initInputFile(firstElements, ismCoder);
    Source sourceB = initInputFile(secondElements, ismCoder);

    List<IsmRecord<WindowedValue<Long>>> firstWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), firstWindow.keySet(), intervalWindow(10));
    List<IsmRecord<WindowedValue<Long>>> secondWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), secondWindow.keySet(), intervalWindow(20));
    List<IsmRecord<WindowedValue<Long>>> thirdWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), thirdWindow.keySet(), intervalWindow(30));
    Source sourceMetaA = initInputFile(firstWindowMapMetadata, ismCoder);
    Source sourceMetaB =
        initInputFile(concat(secondWindowMapMetadata, thirdWindowMapMetadata), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB, sourceMetaA, sourceMetaB);

    List<Callable<Map<BoundedWindow, Map<byte[], Long>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<byte[], Long> firstValues = reader.get(view, intervalWindow(10));
            Map<byte[], Long> secondValues = reader.get(view, intervalWindow(20));
            Map<byte[], Long> thirdValues = reader.get(view, intervalWindow(30));

            verifyMap(
                Maps.transformValues(firstWindow.asMap(), new TransformForMap<Long>()),
                firstValues,
                new ComparatorForMap<Long>());
            verifyMap(
                Maps.transformValues(secondWindow.asMap(), new TransformForMap<Long>()),
                secondValues,
                new ComparatorForMap<Long>());
            verifyMap(
                Maps.transformValues(thirdWindow.asMap(), new TransformForMap<Long>()),
                thirdValues,
                new ComparatorForMap<Long>());

            // Assert that the same value reference was returned showing that it was cached.
            assertSame(firstValues, reader.get(view, intervalWindow(10)));
            assertSame(secondValues, reader.get(view, intervalWindow(20)));
            assertSame(thirdValues, reader.get(view, intervalWindow(30)));

            // Also verify when requesting a window that is not part of the side input
            assertEquals(Collections.EMPTY_MAP, reader.get(view, intervalWindow(40)));

            return ImmutableMap.<BoundedWindow, Map<byte[], Long>>of(
                intervalWindow(10), firstValues,
                intervalWindow(20), secondValues,
                intervalWindow(30), thirdValues);
          });
    }

    List<Future<Map<BoundedWindow, Map<byte[], Long>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, Map<byte[], Long>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, Map<byte[], Long>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Map<byte[], Long>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testMultimap() throws Exception {
    // Note that we purposely use byte[]s as keys to force structural equality testing
    // versus using java equality testing.
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    final ListMultimap<byte[], WindowedValue<Long>> elements =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInGlobalWindow(12L))
            .put(new byte[] {0x01}, valueInGlobalWindow(22L))
            .put(new byte[] {0x02}, valueInGlobalWindow(32L))
            .put(new byte[] {0x03}, valueInGlobalWindow(42L))
            .put(new byte[] {0x04}, valueInGlobalWindow(52L))
            .put(new byte[] {0x05}, valueInGlobalWindow(62L))
            .build();

    final PCollectionView<Map<byte[], Iterable<Long>>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of())))
            .apply(View.asMultimap());

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(ByteArrayCoder.of()),
                GLOBAL_WINDOW_CODER,
                BigEndianLongCoder.of()),
            valueCoder);

    Multimap<Integer, IsmRecord<WindowedValue<Long>>> elementsPerShard = forMap(ismCoder, elements);
    List<IsmRecord<WindowedValue<Long>>> firstElements = new ArrayList<>();
    List<IsmRecord<WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (Map.Entry<Integer, Collection<IsmRecord<WindowedValue<Long>>>> entry :
        elementsPerShard.asMap().entrySet()) {
      if (entry.getKey() % 2 == 0) {
        firstElements.addAll(entry.getValue());
      } else {
        secondElements.addAll(entry.getValue());
      }
    }
    // Ensure that each file will have some records.
    checkState(!firstElements.isEmpty());
    checkState(!secondElements.isEmpty());

    Source sourceA = initInputFile(firstElements, ismCoder);
    Source sourceB = initInputFile(secondElements, ismCoder);

    List<IsmRecord<WindowedValue<Long>>> mapMetadata =
        forMapMetadata(ByteArrayCoder.of(), elements.keySet(), GlobalWindow.INSTANCE);
    Source sourceMeta = initInputFile(mapMetadata, ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB, sourceMeta);

    List<Callable<Map<byte[], Iterable<Long>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<byte[], Iterable<Long>> value = reader.get(view, GlobalWindow.INSTANCE);
            verifyMap(
                Maps.transformValues(elements.asMap(), new TransformForMultimap<Long>()),
                value,
                new ComparatorForMultimap<Long>());
            // Assert that the same value reference was returned showing that it was cached.
            assertSame(reader.get(view, GlobalWindow.INSTANCE), value);

            return value;
          });
    }

    List<Future<Map<byte[], Iterable<Long>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<byte[], Iterable<Long>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<byte[], Iterable<Long>>> result : results) {
      assertSame(value, result.get());
    }
  }

  @Test
  public void testMultimapInWindow() throws Exception {
    // Note that we purposely use byte[]s as keys to force structural equality testing
    // versus using java equality testing.
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    final ListMultimap<byte[], WindowedValue<Long>> firstWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(12L, 10))
            .put(new byte[] {0x01}, valueInIntervalWindow(22L, 10))
            .put(new byte[] {0x02}, valueInIntervalWindow(32L, 10))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> secondWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(42L, 20))
            .put(new byte[] {0x03}, valueInIntervalWindow(52L, 20))
            .put(new byte[] {0x02}, valueInIntervalWindow(62L, 20))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> thirdWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x02}, valueInIntervalWindow(72L, 30))
            .put(new byte[] {0x04}, valueInIntervalWindow(82L, 30))
            .put(new byte[] {0x05}, valueInIntervalWindow(92L, 30))
            .build();

    final PCollectionView<Map<byte[], Iterable<Long>>> view =
        Pipeline.create()
            .apply(Create.empty(KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of())))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMultimap());

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(ByteArrayCoder.of()),
                INTERVAL_WINDOW_CODER,
                BigEndianLongCoder.of()),
            valueCoder);

    Multimap<Integer, IsmRecord<WindowedValue<Long>>> elementsPerShard =
        forMap(ismCoder, firstWindow);
    elementsPerShard.putAll(forMap(ismCoder, secondWindow));
    elementsPerShard.putAll(forMap(ismCoder, thirdWindow));

    List<IsmRecord<WindowedValue<Long>>> firstElements = new ArrayList<>();
    List<IsmRecord<WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (Map.Entry<Integer, Collection<IsmRecord<WindowedValue<Long>>>> entry :
        elementsPerShard.asMap().entrySet()) {
      if (entry.getKey() % 2 == 0) {
        firstElements.addAll(entry.getValue());
      } else {
        secondElements.addAll(entry.getValue());
      }
    }
    // Ensure that each file will have some records.
    checkState(!firstElements.isEmpty());
    checkState(!secondElements.isEmpty());

    Source sourceA = initInputFile(firstElements, ismCoder);
    Source sourceB = initInputFile(secondElements, ismCoder);

    List<IsmRecord<WindowedValue<Long>>> firstWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), firstWindow.keySet(), intervalWindow(10));
    List<IsmRecord<WindowedValue<Long>>> secondWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), secondWindow.keySet(), intervalWindow(20));
    List<IsmRecord<WindowedValue<Long>>> thirdWindowMapMetadata =
        forMapMetadata(ByteArrayCoder.of(), thirdWindow.keySet(), intervalWindow(30));
    Source sourceMetaA = initInputFile(firstWindowMapMetadata, ismCoder);
    Source sourceMetaB =
        initInputFile(concat(secondWindowMapMetadata, thirdWindowMapMetadata), ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB, sourceMetaA, sourceMetaB);

    List<Callable<Map<BoundedWindow, Map<byte[], Iterable<Long>>>>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            Map<byte[], Iterable<Long>> firstValues = reader.get(view, intervalWindow(10));
            Map<byte[], Iterable<Long>> secondValues = reader.get(view, intervalWindow(20));
            Map<byte[], Iterable<Long>> thirdValues = reader.get(view, intervalWindow(30));

            verifyMap(
                Maps.transformValues(firstWindow.asMap(), new TransformForMultimap<Long>()),
                firstValues,
                new ComparatorForMultimap<Long>());
            verifyMap(
                Maps.transformValues(secondWindow.asMap(), new TransformForMultimap<Long>()),
                secondValues,
                new ComparatorForMultimap<Long>());
            verifyMap(
                Maps.transformValues(thirdWindow.asMap(), new TransformForMultimap<Long>()),
                thirdValues,
                new ComparatorForMultimap<Long>());

            // Assert that the same value reference was returned showing that it was cached.
            assertSame(firstValues, reader.get(view, intervalWindow(10)));
            assertSame(secondValues, reader.get(view, intervalWindow(20)));
            assertSame(thirdValues, reader.get(view, intervalWindow(30)));

            // Also verify when requesting a window that is not part of the side input
            assertEquals(Collections.EMPTY_MAP, reader.get(view, intervalWindow(40)));

            return ImmutableMap.<BoundedWindow, Map<byte[], Iterable<Long>>>of(
                intervalWindow(10), firstValues,
                intervalWindow(20), secondValues,
                intervalWindow(30), thirdValues);
          });
    }

    List<Future<Map<BoundedWindow, Map<byte[], Iterable<Long>>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, Map<byte[], Iterable<Long>>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, Map<byte[], Iterable<Long>>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, Map<byte[], Iterable<Long>>> entry : result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testMultimapViewInWindow() throws Exception {
    // Note that we purposely use byte[]s as keys to force structural equality testing
    // versus using java equality testing. Since we want to define a duplicate key for
    // the multimap, we specifically use the same instance of the byte[].
    byte[] duplicateKey = new byte[] {0x01};
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), INTERVAL_WINDOW_CODER);
    final ListMultimap<byte[], WindowedValue<Long>> firstWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(12L, 10))
            .put(duplicateKey, valueInIntervalWindow(22L, 10))
            .put(duplicateKey, valueInIntervalWindow(23L, 10))
            .put(new byte[] {0x02}, valueInIntervalWindow(32L, 10))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> secondWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x00}, valueInIntervalWindow(42L, 20))
            .put(new byte[] {0x03}, valueInIntervalWindow(52L, 20))
            .put(new byte[] {0x02}, valueInIntervalWindow(62L, 20))
            .build();
    final ListMultimap<byte[], WindowedValue<Long>> thirdWindow =
        ImmutableListMultimap.<byte[], WindowedValue<Long>>builder()
            .put(new byte[] {0x02}, valueInIntervalWindow(73L, 30))
            .put(new byte[] {0x04}, valueInIntervalWindow(82L, 30))
            .put(new byte[] {0x05}, valueInIntervalWindow(92L, 30))
            .build();

    final PCollectionView<MultimapView<byte[], WindowedValue<Long>>> view =
        DataflowPortabilityPCollectionView.with(
            new TupleTag<>(),
            FullWindowedValueCoder.of(
                KvCoder.of(ByteArrayCoder.of(), valueCoder), INTERVAL_WINDOW_CODER));

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.of(ByteArrayCoder.of(), INTERVAL_WINDOW_CODER, BigEndianLongCoder.of()),
            valueCoder);

    Multimap<Integer, IsmRecord<WindowedValue<Long>>> elementsPerShard =
        forMap(ismCoder, firstWindow);
    elementsPerShard.putAll(forMap(ismCoder, secondWindow));
    elementsPerShard.putAll(forMap(ismCoder, thirdWindow));

    List<IsmRecord<WindowedValue<Long>>> firstElements = new ArrayList<>();
    List<IsmRecord<WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (Map.Entry<Integer, Collection<IsmRecord<WindowedValue<Long>>>> entry :
        elementsPerShard.asMap().entrySet()) {
      if (entry.getKey() % 2 == 0) {
        firstElements.addAll(entry.getValue());
      } else {
        secondElements.addAll(entry.getValue());
      }
    }
    // Ensure that each file will have some records.
    checkState(!firstElements.isEmpty());
    checkState(!secondElements.isEmpty());

    Source sourceA = initInputFile(firstElements, ismCoder);
    Source sourceB = initInputFile(secondElements, ismCoder);

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), sourceA, sourceB);

    List<Callable<Map<BoundedWindow, MultimapView<byte[], WindowedValue<Long>>>>> tasks =
        new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      tasks.add(
          () -> {
            // Store a strong reference to the returned value so that the logical reference
            // cache is not cleared for this test.
            MultimapView<byte[], WindowedValue<Long>> firstValues =
                reader.get(view, intervalWindow(10));
            MultimapView<byte[], WindowedValue<Long>> secondValues =
                reader.get(view, intervalWindow(20));
            MultimapView<byte[], WindowedValue<Long>> thirdValues =
                reader.get(view, intervalWindow(30));

            for (Map.Entry<byte[], Collection<WindowedValue<Long>>> entry :
                firstWindow.asMap().entrySet()) {
              verifyIterable(entry.getValue(), firstValues.get(entry.getKey()));
            }
            for (Map.Entry<byte[], Collection<WindowedValue<Long>>> entry :
                secondWindow.asMap().entrySet()) {
              verifyIterable(entry.getValue(), secondValues.get(entry.getKey()));
            }
            for (Map.Entry<byte[], Collection<WindowedValue<Long>>> entry :
                thirdWindow.asMap().entrySet()) {
              verifyIterable(entry.getValue(), thirdValues.get(entry.getKey()));
            }

            // Assert that the same value reference was returned showing that it was cached.
            assertSame(firstValues, reader.get(view, intervalWindow(10)));
            assertSame(secondValues, reader.get(view, intervalWindow(20)));
            assertSame(thirdValues, reader.get(view, intervalWindow(30)));

            return ImmutableMap.of(
                intervalWindow(10), firstValues,
                intervalWindow(20), secondValues,
                intervalWindow(30), thirdValues);
          });
    }

    List<Future<Map<BoundedWindow, MultimapView<byte[], WindowedValue<Long>>>>> results =
        pipelineOptions.getExecutorService().invokeAll(tasks);
    Map<BoundedWindow, MultimapView<byte[], WindowedValue<Long>>> value = results.get(0).get();
    // Assert that all threads got back the same reference
    for (Future<Map<BoundedWindow, MultimapView<byte[], WindowedValue<Long>>>> result : results) {
      assertEquals(value, result.get());
      for (Map.Entry<BoundedWindow, MultimapView<byte[], WindowedValue<Long>>> entry :
          result.get().entrySet()) {
        assertSame(value.get(entry.getKey()), entry.getValue());
      }
    }
  }

  @Test
  public void testIterableSideInputReadCounter() throws Exception {
    // These are the expected msec and byte counters:
    CounterUpdate expectedSideInputMsecUpdate =
        new CounterUpdate()
            .setStructuredNameAndMetadata(
                new CounterStructuredNameAndMetadata()
                    .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString()))
                    .setName(
                        new CounterStructuredName()
                            .setOrigin("SYSTEM")
                            .setName("read-sideinput-msecs")
                            .setOriginalStepName("originalName")
                            .setExecutionStepName("stageName")
                            .setOriginalRequestingStepName("originalName2")
                            .setInputIndex(1)))
            .setCumulative(true)
            .setInteger(new SplitInt64().setHighBits(0).setLowBits(0L));
    CounterName expectedCounterName =
        CounterName.named("read-sideinput-byte-count")
            .withOriginalName(operationContext.nameContext())
            .withOrigin("SYSTEM")
            .withOriginalRequestingStepName("originalName2")
            .withInputIndex(1);

    // Test startup:
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.of(GLOBAL_WINDOW_CODER, BigEndianLongCoder.of()), valueCoder);

    // Create a new state, which represents a step that receives the side input.
    DataflowExecutionState state2 =
        executionContext
            .getExecutionStateRegistry()
            .getState(
                NameContext.create("stageName", "originalName2", "systemName2", "userName2"),
                "process",
                null,
                NoopProfileScope.NOOP);

    final List<KV<Long, WindowedValue<Long>>> firstElements =
        Arrays.asList(KV.of(0L, valueInGlobalWindow(0L)));
    final List<KV<Long, WindowedValue<Long>>> secondElements = new ArrayList<>();
    for (long i = 0; i < 100; i++) {
      secondElements.add(KV.of(i, valueInGlobalWindow(i * 10)));
    }

    final PCollectionView<Iterable<Long>> view =
        Pipeline.create().apply(Create.empty(VarLongCoder.of())).apply(View.asIterable());

    Source sourceA = initInputFile(fromKvsForList(firstElements), ismCoder);
    Source sourceB = initInputFile(fromKvsForList(secondElements), ismCoder);
    try (Closeable state2Closeable =
        executionContext.getExecutionStateTracker().enterState(state2)) {
      final IsmSideInputReader reader =
          serialSideInputReader(view.getTagInternal().getId(), sourceA, sourceB);
      // Store a strong reference to the returned value so that the logical reference
      // cache is not cleared for this test.
      Iterable<Long> value = reader.get(view, GlobalWindow.INSTANCE);
      verifyIterable(toValueList(concat(firstElements, secondElements)), value);
      // Assert that the same value reference was returned showing that it was cached.
      assertSame(reader.get(view, GlobalWindow.INSTANCE), value);

      Iterable<CounterUpdate> counterUpdates =
          executionContext.getExecutionStateRegistry().extractUpdates(true);
      assertThat(counterUpdates, hasItem(expectedSideInputMsecUpdate));
      Counter<?, ?> expectedCounter = counterFactory.getExistingCounter(expectedCounterName);
      assertNotNull(expectedCounter);
    }
  }

  @Test
  public void testIsmReaderReferenceCaching() throws Exception {
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValue.getFullCoder(VarLongCoder.of(), GLOBAL_WINDOW_CODER);
    final WindowedValue<Long> element = valueInGlobalWindow(42L);
    final PCollectionView<Long> view =
        Pipeline.create().apply(Create.empty(VarLongCoder.of())).apply(View.asSingleton());

    final Source source =
        initInputFile(
            fromValues(Arrays.asList(element)),
            IsmRecordCoder.of(1, 0, ImmutableList.<Coder<?>>of(GLOBAL_WINDOW_CODER), valueCoder));
    final Source emptySource =
        initInputFile(
            fromValues(Arrays.asList()),
            IsmRecordCoder.of(1, 0, ImmutableList.<Coder<?>>of(GLOBAL_WINDOW_CODER), valueCoder));

    final IsmSideInputReader reader =
        sideInputReader(view.getTagInternal().getId(), source, emptySource);

    assertTrue(reader.tagToIsmReaderMap.containsKey(view.getTagInternal()));
    assertEquals(1, reader.tagToIsmReaderMap.get(view.getTagInternal()).size());
    assertEquals(
        FileSystems.matchSingleFileSpec(getString(source.getSpec(), WorkerPropertyNames.FILENAME))
            .resourceId(),
        reader.tagToIsmReaderMap.get(view.getTagInternal()).get(0).getResourceId());
    assertTrue(reader.tagToEmptyIsmReaderMap.containsKey(view.getTagInternal()));
    assertEquals(1, reader.tagToEmptyIsmReaderMap.get(view.getTagInternal()).size());
    assertEquals(
        FileSystems.matchSingleFileSpec(
                getString(emptySource.getSpec(), WorkerPropertyNames.FILENAME))
            .resourceId(),
        reader.tagToEmptyIsmReaderMap.get(view.getTagInternal()).get(0).getResourceId());
  }

  private static class TransformForMap<T> implements Function<Collection<WindowedValue<T>>, T> {
    @Override
    public T apply(Collection<WindowedValue<T>> input) {
      return Iterables.getOnlyElement(input).getValue();
    }
  }

  private static class TransformForMultimap<T>
      implements Function<Collection<WindowedValue<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Collection<WindowedValue<T>> input) {
      return Iterables.transform(input, WindowedValue::getValue);
    }
  }

  private static class ComparatorForMap<T> implements Comparator<T> {
    @Override
    public int compare(T o1, T o2) {
      assertEquals(o1, o2);
      return 0;
    }
  }

  private static class ComparatorForMultimap<T> implements Comparator<Iterable<T>> {
    @Override
    public int compare(Iterable<T> o1, Iterable<T> o2) {
      ArrayList<T> elements = new ArrayList<>();
      Iterables.addAll(elements, o2);
      @SuppressWarnings({"unchecked", "rawtypes"})
      Matcher<Iterable<T>> matcher = (Matcher) containsInAnyOrder(elements.toArray());
      assertThat(o1, matcher);
      return 0;
    }
  }

  private <T> void verifyIterable(Collection<T> expectedElements, Iterable<T> actual) {
    Iterator<T> actualIterator = actual.iterator();
    // Test with using hasNext
    for (T expectedElement : expectedElements) {
      assertTrue(actualIterator.hasNext());
      assertEquals(expectedElement, actualIterator.next());
    }
    assertFalse(actualIterator.hasNext());
    try {
      actualIterator.next();
      fail("NoSuchElementException was expected");
    } catch (NoSuchElementException expected) {
    }

    actualIterator = actual.iterator();
    // Test without using hasNext
    for (T expectedElement : expectedElements) {
      assertEquals(expectedElement, actualIterator.next());
    }
    try {
      actualIterator.next();
      fail("NoSuchElementException was expected");
    } catch (NoSuchElementException expected) {
    }
  }

  private <T> void verifyList(List<T> expected, List<T> actual) {
    assertEquals(expected.size(), actual.size());

    List<Integer> iterationOrder = new ArrayList<Integer>();
    Random random = new Random(1892389023490L);
    for (int i = 0; i < expected.size(); ++i) {
      iterationOrder.add(i);
    }
    Collections.shuffle(iterationOrder, random);

    // Test random iteration order
    for (int index : iterationOrder) {
      assertEquals(expected.get(index), actual.get(index));
    }

    // Test the iterator
    verifyIterable(expected, actual);

    // Test the list iterator
    for (int index : iterationOrder) {
      ListIterator<T> expectedListIterator = expected.listIterator(index);
      ListIterator<T> actualListIterator = actual.listIterator(index);

      assertEquals(expectedListIterator.hasNext(), actualListIterator.hasNext());
      assertEquals(expectedListIterator.hasPrevious(), actualListIterator.hasPrevious());
      assertEquals(expectedListIterator.nextIndex(), actualListIterator.nextIndex());
      assertEquals(expectedListIterator.previousIndex(), actualListIterator.previousIndex());

      if (expectedListIterator.hasNext()) {
        // Move to next element and compare
        assertEquals(expectedListIterator.next(), actualListIterator.next());
        assertEquals(expectedListIterator.hasNext(), actualListIterator.hasNext());
        assertEquals(expectedListIterator.hasPrevious(), actualListIterator.hasPrevious());
        assertEquals(expectedListIterator.nextIndex(), actualListIterator.nextIndex());
        assertEquals(expectedListIterator.previousIndex(), actualListIterator.previousIndex());

        // Move to previous element and compare
        assertEquals(expectedListIterator.previous(), actualListIterator.previous());
        assertEquals(expectedListIterator.hasNext(), actualListIterator.hasNext());
        assertEquals(expectedListIterator.hasPrevious(), actualListIterator.hasPrevious());
        assertEquals(expectedListIterator.nextIndex(), actualListIterator.nextIndex());
        assertEquals(expectedListIterator.previousIndex(), actualListIterator.previousIndex());
      }

      if (expectedListIterator.hasPrevious()) {
        // Move to previous element and compare
        assertEquals(expectedListIterator.previous(), actualListIterator.previous());
        assertEquals(expectedListIterator.hasNext(), actualListIterator.hasNext());
        assertEquals(expectedListIterator.hasPrevious(), actualListIterator.hasPrevious());
        assertEquals(expectedListIterator.nextIndex(), actualListIterator.nextIndex());
        assertEquals(expectedListIterator.previousIndex(), actualListIterator.previousIndex());

        // Move to next element and compare
        assertEquals(expectedListIterator.next(), actualListIterator.next());
        assertEquals(expectedListIterator.hasNext(), actualListIterator.hasNext());
        assertEquals(expectedListIterator.hasPrevious(), actualListIterator.hasPrevious());
        assertEquals(expectedListIterator.nextIndex(), actualListIterator.nextIndex());
        assertEquals(expectedListIterator.previousIndex(), actualListIterator.previousIndex());
      }
    }
  }

  // TODO(https://github.com/apache/beam/issues/21294): Add assertions on contains() calls
  @SuppressWarnings("ReturnValueIgnored")
  private static <T> void verifyMap(
      Map<byte[], T> expectedMap, Map<byte[], T> mapView, Comparator<T> valueComparator) {
    List<Entry<byte[], T>> expectedElements = new ArrayList<>(expectedMap.entrySet());

    Random random = new Random(1237812387L);

    // Verify the size
    assertEquals(expectedMap.size(), mapView.size());

    // Verify random look ups
    Collections.shuffle(expectedElements, random);
    for (Entry<byte[], T> expected : expectedElements) {
      assertTrue(valueComparator.compare(expected.getValue(), mapView.get(expected.getKey())) == 0);
    }

    // Verify random contains
    Collections.shuffle(expectedElements, random);
    for (Entry<byte[], T> expected : expectedElements) {
      assertTrue(mapView.containsKey(expected.getKey()));
    }

    // Verify random key set contains
    Collections.shuffle(expectedElements, random);
    Set<byte[]> mapViewKeySet = mapView.keySet();
    for (Entry<byte[], T> expected : expectedElements) {
      assertTrue(mapViewKeySet.contains(expected.getKey()));
    }

    // Verify key set iterator
    Iterator<byte[]> mapViewKeySetIterator = mapView.keySet().iterator();
    assertEquals(expectedElements.size(), Iterators.size(mapViewKeySetIterator));
    try {
      mapViewKeySetIterator.next();
      fail("Expected to have thrown NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }

    // Verify random entry set contains
    Collections.shuffle(expectedElements, random);
    Set<Map.Entry<byte[], T>> mapViewEntrySet = mapView.entrySet();
    for (Entry<byte[], T> expected : expectedElements) {
      mapViewEntrySet.contains(new SimpleImmutableEntry<>(expected.getKey(), expected.getValue()));
    }

    // Verify entry set iterator
    Iterator<Map.Entry<byte[], T>> mapViewEntrySetIterator = mapView.entrySet().iterator();
    assertEquals(expectedElements.size(), Iterators.size(mapViewEntrySetIterator));
    try {
      mapViewEntrySetIterator.next();
      fail("Expected to have thrown NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }

    // Verify random value collection contains
    Collections.shuffle(expectedElements, random);
    Collection<T> mapViewValues = mapView.values();
    for (Entry<byte[], T> expected : expectedElements) {
      mapViewValues.contains(expected.getValue());
    }

    // Verify entry set iterator
    Iterator<T> mapViewValuesIterator = mapView.values().iterator();
    assertEquals(expectedElements.size(), Iterators.size(mapViewValuesIterator));
    try {
      mapViewValuesIterator.next();
      fail("Expected to have thrown NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }
  }

  WindowedValue<Long> valueInIntervalWindow(long value, long startOfWindow) {
    return WindowedValue.of(
        value, new Instant(startOfWindow), intervalWindow(startOfWindow), PaneInfo.NO_FIRING);
  }

  private static IntervalWindow intervalWindow(long start) {
    return new IntervalWindow(new Instant(start), new Instant(start + 1));
  }

  private static BoundedWindow windowOf(WindowedValue<?> windowedValue) {
    return windowedValue.getWindows().iterator().next();
  }

  <T> List<IsmRecord<WindowedValue<T>>> fromValues(Iterable<WindowedValue<T>> elements) {
    List<IsmRecord<WindowedValue<T>>> rval = new ArrayList<>();
    for (WindowedValue<T> element : elements) {
      rval.add(IsmRecord.of(ImmutableList.of(windowOf(element)), element));
    }
    return rval;
  }

  <K, V> List<IsmRecord<WindowedValue<V>>> fromKvsForList(
      Iterable<KV<K, WindowedValue<V>>> elements) {
    List<IsmRecord<WindowedValue<V>>> rval = new ArrayList<>();
    for (KV<K, WindowedValue<V>> element : elements) {
      rval.add(
          IsmRecord.of(
              ImmutableList.of(windowOf(element.getValue()), element.getKey()),
              element.getValue()));
    }
    return rval;
  }

  /**
   * Note that it is important that the return value if split is only split on shard boundaries
   * because it is expected that each shard id only appears in one source.
   *
   * <p>Each windowed value is expected to be within the same window.
   */
  <K, V> Multimap<Integer, IsmRecord<WindowedValue<V>>> forMap(
      IsmRecordCoder<WindowedValue<V>> coder, ListMultimap<K, WindowedValue<V>> elements)
      throws Exception {

    Multimap<Integer, IsmRecord<WindowedValue<V>>> rval =
        TreeMultimap.create(Ordering.natural(), new IsmReaderTest.IsmRecordKeyComparator<>(coder));

    for (K key : elements.keySet()) {
      long i = 0;
      for (WindowedValue<V> value : elements.get(key)) {
        IsmRecord<WindowedValue<V>> record =
            IsmRecord.of(ImmutableList.of(key, windowOf(value), i), value);
        rval.put(coder.hash(record.getKeyComponents()), record);
        i += 1L;
      }
    }

    return rval;
  }

  /** Each windowed value is expected to be within the same window. */
  <K, V> List<IsmRecord<WindowedValue<V>>> forMapMetadata(
      Coder<K> keyCoder, Collection<K> keys, BoundedWindow window) throws Exception {

    List<IsmRecord<WindowedValue<V>>> rval = new ArrayList<>();
    // Add the size metadata record
    rval.add(
        IsmRecord.<WindowedValue<V>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), window, 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), (long) keys.size())));

    // Add the positional entries for each key
    long i = 1L;
    for (K key : keys) {
      rval.add(
          IsmRecord.<WindowedValue<V>>meta(
              ImmutableList.of(IsmFormat.getMetadataKey(), window, i),
              CoderUtils.encodeToByteArray(keyCoder, key)));
      i += 1L;
    }
    return rval;
  }

  <K, V> ArrayList<V> toValueList(Iterable<KV<K, WindowedValue<V>>> windowedValues) {
    ArrayList<V> values = new ArrayList<>();
    for (KV<K, WindowedValue<V>> kvWindowedValue : windowedValues) {
      values.add(kvWindowedValue.getValue().getValue());
    }
    return values;
  }

  @SuppressWarnings("unchecked")
  private static RandomAccessData encodeKeyPortion(IsmRecordCoder<?> coder, IsmRecord<?> record)
      throws IOException {
    RandomAccessData keyBytes = new RandomAccessData();
    for (int i = 0; i < coder.getKeyComponentCoders().size(); ++i) {
      coder.getKeyComponentCoder(i).encode(record.getKeyComponent(i), keyBytes.asOutputStream());
    }
    return keyBytes;
  }

  /** Write input elements to a new temporary file and return the corresponding IsmSource. */
  private <K, V> Source initInputFile(
      Iterable<IsmRecord<WindowedValue<V>>> elements, IsmRecordCoder<WindowedValue<V>> coder)
      throws Exception {
    return initInputFile(elements, coder, tmpFolder.newFile().getPath());
  }

  /** Write input elements to the given file and return the corresponding IsmSource. */
  private <K, V> Source initInputFile(
      Iterable<IsmRecord<WindowedValue<V>>> elements,
      IsmRecordCoder<WindowedValue<V>> coder,
      String tmpFilePath)
      throws Exception {
    // Group the keys by shard and sort the values within a shard by the composite key.
    Map<Integer, SortedMap<RandomAccessData, IsmRecord<WindowedValue<V>>>> writeOrder =
        new HashMap<>();
    for (IsmRecord<WindowedValue<V>> element : elements) {
      int shardId = coder.hash(element.getKeyComponents());
      if (!writeOrder.containsKey(shardId)) {
        writeOrder.put(
            shardId,
            new TreeMap<RandomAccessData, IsmRecord<WindowedValue<V>>>(
                RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR));
      }
      RandomAccessData data = encodeKeyPortion(coder, element);
      writeOrder.get(shardId).put(data, element);
    }

    IsmSink<WindowedValue<V>> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFilePath, false), coder, BLOOM_FILTER_SIZE_LIMIT);
    try (SinkWriter<WindowedValue<IsmRecord<WindowedValue<V>>>> writer = sink.writer()) {
      for (Entry<Integer, SortedMap<RandomAccessData, IsmRecord<WindowedValue<V>>>> entry :
          writeOrder.entrySet()) {
        for (IsmRecord<WindowedValue<V>> record : entry.getValue().values()) {
          writer.add(new ValueInEmptyWindows<>(record));
        }
      }
    }
    return newIsmSource(coder, tmpFilePath);
  }

  /** Returns a new Source for the given ISM file using the specified coder. */
  private <K, V> Source newIsmSource(IsmRecordCoder<WindowedValue<V>> coder, String tmpFilePath) {
    Source source = new Source();
    source.setCodec(
        CloudObjects.asCloudObject(
            WindowedValue.getFullCoder(coder, GLOBAL_WINDOW_CODER), /*sdkComponents=*/ null));
    source.setSpec(new HashMap<String, Object>());
    source.getSpec().put(PropertyNames.OBJECT_TYPE_NAME, "IsmSource");
    source.getSpec().put(WorkerPropertyNames.FILENAME, tmpFilePath);
    return source;
  }

  private SideInputInfo toSideInputInfo(String tagId, Source... sources) {
    SideInputInfo sideInputInfo = new SideInputInfo();
    sideInputInfo.setTag(tagId);
    sideInputInfo.setKind(new HashMap<String, Object>());
    if (sources.length == 1) {
      sideInputInfo.getKind().put(PropertyNames.OBJECT_TYPE_NAME, "singleton");
    } else {
      sideInputInfo.getKind().put(PropertyNames.OBJECT_TYPE_NAME, "collection");
    }
    sideInputInfo.setSources(new ArrayList<>(Arrays.asList(sources)));
    return sideInputInfo;
  }

  private IsmSideInputReader serialSideInputReader(String tagId, Source... sources)
      throws Exception {
    return IsmSideInputReader.forTest(
        Arrays.asList(toSideInputInfo(tagId, sources)),
        pipelineOptions,
        executionContext,
        ReaderRegistry.defaultRegistry(),
        operationContext,
        MoreExecutors.newDirectExecutorService());
  }

  private IsmSideInputReader sideInputReader(String tagId, Source... sources) throws Exception {
    return IsmSideInputReader.of(
        Arrays.asList(toSideInputInfo(tagId, sources)),
        pipelineOptions,
        executionContext,
        ReaderRegistry.defaultRegistry(),
        operationContext);
  }
}
