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
package org.apache.beam.runners.direct;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.apache.beam.runners.direct.DirectGraphs.getProducer;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.NeverDeduplicator;
import org.apache.beam.runners.direct.UnboundedReadEvaluatorFactory.UnboundedSourceShard;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ContiguousSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.DiscreteDomain;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.LinkedListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UnboundedReadEvaluatorFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
})
public class UnboundedReadEvaluatorFactoryTest {
  private PCollection<Long> longs;
  private UnboundedReadEvaluatorFactory factory;
  private EvaluationContext context;
  private UncommittedBundle<Long> output;

  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  private UnboundedSource<Long, ?> source;
  private DirectGraph graph;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    source = CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    longs = p.apply(Read.from(source));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    context = mock(EvaluationContext.class);
    factory = new UnboundedReadEvaluatorFactory(context, p.getOptions());
    output = bundleFactory.createBundle(longs);
    graph = DirectGraphs.getGraph(p);
    when(context.createBundle(longs)).thenReturn(output);
  }

  @Test
  public void generatesInitialSplits() throws Exception {
    when(context.createRootBundle()).thenAnswer(invocation -> bundleFactory.createRootBundle());

    int numSplits = 5;
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context, p.getOptions())
            .getInitialInputs(graph.getProducer(longs), numSplits);
    // CountingSource.unbounded has very good splitting behavior
    assertThat(initialInputs, hasSize(numSplits));

    int readPerSplit = 100;
    int totalSize = numSplits * readPerSplit;
    Set<Long> expectedOutputs =
        ContiguousSet.create(Range.closedOpen(0L, (long) totalSize), DiscreteDomain.longs());

    Collection<Long> readItems = new ArrayList<>(totalSize);
    for (CommittedBundle<?> initialInput : initialInputs) {
      CommittedBundle<UnboundedSourceShard<Long, ?>> shardBundle =
          (CommittedBundle<UnboundedSourceShard<Long, ?>>) initialInput;
      WindowedValue<UnboundedSourceShard<Long, ?>> shard =
          Iterables.getOnlyElement(shardBundle.getElements());
      assertThat(shard.getTimestamp(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
      assertThat(shard.getWindows(), Matchers.contains(GlobalWindow.INSTANCE));
      UnboundedSource<Long, ?> shardSource = shard.getValue().getSource();
      readItems.addAll(
          SourceTestUtils.readNItemsFromUnstartedReader(
              shardSource.createReader(
                  PipelineOptionsFactory.create(), null /* No starting checkpoint */),
              readPerSplit));
    }
    assertThat(readItems, containsInAnyOrder(expectedOutputs.toArray(new Long[0])));
  }

  @Test
  public void unboundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());

    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context, p.getOptions())
            .getInitialInputs(graph.getProducer(longs), 1);

    CommittedBundle<?> inputShards = Iterables.getOnlyElement(initialInputs);
    UnboundedSourceShard<Long, ?> inputShard =
        (UnboundedSourceShard<Long, ?>)
            Iterables.getOnlyElement(inputShards.getElements()).getValue();
    TransformEvaluator<? super UnboundedSourceShard<Long, ?>> evaluator =
        factory.forApplication(graph.getProducer(longs), inputShards);

    evaluator.processElement((WindowedValue) Iterables.getOnlyElement(inputShards.getElements()));
    TransformResult<? super UnboundedSourceShard<Long, ?>> result = evaluator.finishBundle();

    WindowedValue<? super UnboundedSourceShard<Long, ?>> residual =
        Iterables.getOnlyElement(result.getUnprocessedElements());
    assertThat(residual.getTimestamp(), Matchers.lessThan(DateTime.now().toInstant()));
    UnboundedSourceShard<Long, ?> residualShard =
        (UnboundedSourceShard<Long, ?>) residual.getValue();
    assertThat(residualShard.getSource(), equalTo(inputShard.getSource()));
    assertThat(residualShard.getCheckpoint(), not(nullValue()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));
  }

  @Test
  public void unboundedSourceWithDuplicatesMultipleCalls() throws Exception {
    Long[] outputs = new Long[20];
    for (long i = 0L; i < 20L; i++) {
      outputs[(int) i] = i % 5L;
    }
    TestUnboundedSource<Long> source = new TestUnboundedSource<>(BigEndianLongCoder.of(), outputs);
    source.dedupes = true;

    PCollection<Long> pcollection = p.apply(Read.from(source));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    AppliedPTransform<?, ?, ?> sourceTransform = getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context, p.getOptions())
            .getInitialInputs(sourceTransform, 1);

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);
    CommittedBundle<?> inputBundle = Iterables.getOnlyElement(initialInputs);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> evaluator =
        factory.forApplication(sourceTransform, inputBundle);

    for (WindowedValue<?> value : inputBundle.getElements()) {
      evaluator.processElement(
          (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>) value);
    }
    TransformResult<UnboundedSourceShard<Long, TestCheckpointMark>> result =
        evaluator.finishBundle();
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(1L), tgw(2L), tgw(4L), tgw(3L), tgw(0L)));

    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> secondEvaluator =
        factory.forApplication(sourceTransform, inputBundle);
    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> residual =
        (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>)
            Iterables.getOnlyElement(result.getUnprocessedElements());
    secondEvaluator.processElement(residual);
    secondEvaluator.finishBundle();
    assertThat(secondOutput.commit(Instant.now()).getElements(), Matchers.emptyIterable());
  }

  @Test
  public void noElementsAvailableReaderIncludedInResidual() throws Exception {
    // Read with a very slow rate so by the second read there are no more elements
    PCollection<Long> pcollection =
        p.apply(Read.from(new TestUnboundedSource<>(VarLongCoder.of(), 1L)));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    AppliedPTransform<?, ?, ?> sourceTransform = getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context, p.getOptions())
            .getInitialInputs(sourceTransform, 1);

    // Process the initial shard. This might produce some output, and will produce a residual shard
    // which should produce no output when read from within the following day.
    when(context.createBundle(pcollection)).thenReturn(bundleFactory.createBundle(pcollection));
    CommittedBundle<?> inputBundle = Iterables.getOnlyElement(initialInputs);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> evaluator =
        factory.forApplication(sourceTransform, inputBundle);
    for (WindowedValue<?> value : inputBundle.getElements()) {
      evaluator.processElement(
          (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>) value);
    }
    TransformResult<UnboundedSourceShard<Long, TestCheckpointMark>> result =
        evaluator.finishBundle();

    // Read from the residual of the first read. This should not produce any output, but should
    // include a residual shard in the result.
    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> secondEvaluator =
        factory.forApplication(sourceTransform, inputBundle);
    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> residual =
        (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>)
            Iterables.getOnlyElement(result.getUnprocessedElements());
    secondEvaluator.processElement(residual);

    TransformResult<UnboundedSourceShard<Long, TestCheckpointMark>> secondResult =
        secondEvaluator.finishBundle();

    // Sanity check that nothing was output (The test would have to run for more than a day to do
    // so correctly.)
    assertThat(secondOutput.commit(Instant.now()).getElements(), Matchers.emptyIterable());

    // Test that even though the reader produced no outputs, there is still a residual shard with
    // the updated watermark.
    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> unprocessed =
        (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>)
            Iterables.getOnlyElement(secondResult.getUnprocessedElements());
    assertThat(unprocessed.getTimestamp(), Matchers.greaterThan(residual.getTimestamp()));
    assertThat(unprocessed.getValue().getExistingReader(), not(nullValue()));
  }

  @Test
  public void evaluatorReusesReaderAndClosesAtTheEnd() throws Exception {
    int numElements = 1000;
    ContiguousSet<Long> elems =
        ContiguousSet.create(Range.openClosed(0L, (long) numElements), DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]));
    source.advanceWatermarkToInfinity = true;

    PCollection<Long> pcollection = p.apply(Read.from(source));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    DirectGraph graph = DirectGraphs.getGraph(p);
    AppliedPTransform<?, ?, ?> sourceTransform = graph.getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    UncommittedBundle<Long> output = mock(UncommittedBundle.class);
    when(context.createBundle(pcollection)).thenReturn(output);

    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> shard =
        WindowedValue.valueInGlobalWindow(
            UnboundedSourceShard.unstarted(source, NeverDeduplicator.create()));
    CommittedBundle<UnboundedSourceShard<Long, TestCheckpointMark>> inputBundle =
        bundleFactory
            .<UnboundedSourceShard<Long, TestCheckpointMark>>createRootBundle()
            .add(shard)
            .commit(Instant.now());
    UnboundedReadEvaluatorFactory factory =
        new UnboundedReadEvaluatorFactory(context, p.getOptions(), 1.0 /* Always reuse */);
    new UnboundedReadEvaluatorFactory.InputProvider(context, p.getOptions())
        .getInitialInputs(sourceTransform, 1);

    CommittedBundle<UnboundedSourceShard<Long, TestCheckpointMark>> residual = inputBundle;

    do {
      TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> evaluator =
          factory.forApplication(sourceTransform, residual);
      evaluator.processElement(Iterables.getOnlyElement(residual.getElements()));
      TransformResult<UnboundedSourceShard<Long, TestCheckpointMark>> result =
          evaluator.finishBundle();
      residual =
          inputBundle.withElements(
              (Iterable<WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>>)
                  result.getUnprocessedElements());
    } while (!Iterables.isEmpty(residual.getElements()));

    verify(output, times(numElements)).add(any());
    assertThat(TestUnboundedSource.readerCreatedCount, equalTo(1));
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(1));
  }

  @Test
  public void evaluatorClosesReaderAndResumesFromCheckpoint() throws Exception {
    ContiguousSet<Long> elems = ContiguousSet.create(Range.closed(0L, 20L), DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]));

    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = DirectGraphs.getGraph(p).getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> shard =
        WindowedValue.valueInGlobalWindow(
            UnboundedSourceShard.unstarted(source, NeverDeduplicator.create()));
    CommittedBundle<UnboundedSourceShard<Long, TestCheckpointMark>> inputBundle =
        bundleFactory
            .<UnboundedSourceShard<Long, TestCheckpointMark>>createRootBundle()
            .add(shard)
            .commit(Instant.now());
    UnboundedReadEvaluatorFactory factory =
        new UnboundedReadEvaluatorFactory(context, p.getOptions(), 0.0 /* never reuse */);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> evaluator =
        factory.forApplication(sourceTransform, inputBundle);
    evaluator.processElement(shard);
    TransformResult<UnboundedSourceShard<Long, TestCheckpointMark>> result =
        evaluator.finishBundle();

    CommittedBundle<UnboundedSourceShard<Long, TestCheckpointMark>> residual =
        inputBundle.withElements(
            (Iterable<WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>>)
                result.getUnprocessedElements());

    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> secondEvaluator =
        factory.forApplication(sourceTransform, residual);
    secondEvaluator.processElement(Iterables.getOnlyElement(residual.getElements()));
    secondEvaluator.finishBundle();

    assertThat(TestUnboundedSource.readerClosedCount, equalTo(2));
    assertThat(
        Iterables.getOnlyElement(residual.getElements()).getValue().getCheckpoint().isFinalized(),
        is(true));
  }

  @Test
  public void evaluatorThrowsInCloseRethrows() throws Exception {
    ContiguousSet<Long> elems = ContiguousSet.create(Range.closed(0L, 20L), DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]))
            .throwsOnClose();

    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = DirectGraphs.getGraph(p).getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> shard =
        WindowedValue.valueInGlobalWindow(
            UnboundedSourceShard.unstarted(source, NeverDeduplicator.create()));
    CommittedBundle<UnboundedSourceShard<Long, TestCheckpointMark>> inputBundle =
        bundleFactory
            .<UnboundedSourceShard<Long, TestCheckpointMark>>createRootBundle()
            .add(shard)
            .commit(Instant.now());
    UnboundedReadEvaluatorFactory factory =
        new UnboundedReadEvaluatorFactory(context, p.getOptions(), 0.0 /* never reuse */);
    TransformEvaluator<UnboundedSourceShard<Long, TestCheckpointMark>> evaluator =
        factory.forApplication(sourceTransform, inputBundle);
    thrown.expect(IOException.class);
    thrown.expectMessage("throws on close");
    evaluator.processElement(shard);
  }

  @Test // before this was throwing a NPE
  public void emptySource() throws Exception {
    TestUnboundedSource.readerClosedCount = 0;
    final TestUnboundedSource<String> source = new TestUnboundedSource<>(StringUtf8Coder.of());
    source.advanceWatermarkToInfinity = true;
    processElement(source);
    assertEquals(1, TestUnboundedSource.readerClosedCount);
    TestUnboundedSource.readerClosedCount = 0; // reset
  }

  @Test(expected = IOException.class)
  public void sourceThrowingException() throws Exception {
    final TestUnboundedSource<String> source = new TestUnboundedSource<>(StringUtf8Coder.of());
    source.advanceWatermarkToInfinity = true;
    source.throwOnClose = true;
    processElement(source);
  }

  private void processElement(final TestUnboundedSource<String> source) throws Exception {
    final EvaluationContext context =
        EvaluationContext.create(
            MockClock.fromInstant(Instant.now()),
            CloningBundleFactory.create(),
            DirectGraph.create(
                emptyMap(), emptyMap(), LinkedListMultimap.create(), emptySet(), emptyMap()),
            emptySet(),
            Executors.newCachedThreadPool());
    final UnboundedReadEvaluatorFactory factory =
        new UnboundedReadEvaluatorFactory(context, p.getOptions());

    final SplittableParDo.PrimitiveUnboundedRead<String> unbounded =
        new SplittableParDo.PrimitiveUnboundedRead(Read.from(source));
    final Pipeline pipeline = Pipeline.create(p.getOptions());
    final PCollection<String> pCollection = pipeline.apply(unbounded);
    final AppliedPTransform<
            PBegin, PCollection<String>, SplittableParDo.PrimitiveUnboundedRead<String>>
        application =
            AppliedPTransform.of(
                "test",
                new HashMap<>(),
                singletonMap(new TupleTag(), pCollection),
                unbounded,
                ResourceHints.create(),
                pipeline);
    final TransformEvaluator<UnboundedSourceShard<String, TestCheckpointMark>> evaluator =
        factory.forApplication(application, null);
    final UnboundedSource.UnboundedReader<String> reader =
        source.createReader(p.getOptions(), null);
    final UnboundedSourceShard<String, TestCheckpointMark> shard =
        UnboundedSourceShard.of(source, new NeverDeduplicator(), reader, null);
    final WindowedValue<UnboundedSourceShard<String, TestCheckpointMark>> value =
        WindowedValue.of(
            shard, BoundedWindow.TIMESTAMP_MAX_VALUE, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
    TestUnboundedSource.readerClosedCount = 0;
    evaluator.processElement(value);
  }

  /**
   * A terse alias for producing timestamped longs in the {@link GlobalWindow}, where the timestamp
   * is the epoch offset by the value of the element.
   */
  private static WindowedValue<Long> tgw(Long elem) {
    return WindowedValue.timestampedValueInGlobalWindow(elem, new Instant(elem));
  }

  private static class LongToInstantFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }

  private static class TestUnboundedSource<T> extends UnboundedSource<T, TestCheckpointMark> {
    private static int getWatermarkCalls = 0;

    static int readerCreatedCount;
    static int readerClosedCount;
    static int readerAdvancedCount;
    private final Coder<T> coder;
    private final List<T> elems;
    private boolean dedupes = false;
    private boolean advanceWatermarkToInfinity = false; // After reaching end of input.
    private boolean throwOnClose;

    public TestUnboundedSource(Coder<T> coder, T... elems) {
      this(coder, false, Arrays.asList(elems));
    }

    private TestUnboundedSource(Coder<T> coder, boolean throwOnClose, List<T> elems) {
      readerCreatedCount = 0;
      readerClosedCount = 0;
      readerAdvancedCount = 0;
      this.coder = coder;
      this.elems = elems;
      this.throwOnClose = throwOnClose;
    }

    @Override
    public List<? extends UnboundedSource<T, TestCheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public UnboundedSource.UnboundedReader<T> createReader(
        PipelineOptions options, @Nullable TestCheckpointMark checkpointMark) {
      checkState(
          checkpointMark == null || checkpointMark.decoded,
          "Cannot resume from a checkpoint that has not been decoded");
      readerCreatedCount++;
      return new TestUnboundedReader(elems, checkpointMark == null ? -1 : checkpointMark.index);
    }

    @Override
    public @Nullable Coder<TestCheckpointMark> getCheckpointMarkCoder() {
      return new TestCheckpointMark.Coder();
    }

    @Override
    public boolean requiresDeduping() {
      return dedupes;
    }

    @Override
    public Coder<T> getOutputCoder() {
      return coder;
    }

    public TestUnboundedSource<T> throwsOnClose() {
      return new TestUnboundedSource<>(coder, true, elems);
    }

    private class TestUnboundedReader extends UnboundedReader<T> {
      private final List<T> elems;
      private int index;
      private boolean closed = false;

      public TestUnboundedReader(List<T> elems, int startIndex) {
        this.elems = elems;
        this.index = startIndex;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        readerAdvancedCount++;
        if (index + 1 < elems.size()) {
          index++;
          return true;
        }
        return false;
      }

      @Override
      public Instant getWatermark() {
        getWatermarkCalls++;
        if (index + 1 == elems.size() && TestUnboundedSource.this.advanceWatermarkToInfinity) {
          return BoundedWindow.TIMESTAMP_MAX_VALUE;
        } else {
          return new Instant(index + getWatermarkCalls);
        }
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new TestCheckpointMark(index);
      }

      @Override
      public UnboundedSource<T, ?> getCurrentSource() {
        return TestUnboundedSource.this;
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return elems.get(index);
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(index);
      }

      @Override
      public byte[] getCurrentRecordId() {
        try {
          return CoderUtils.encodeToByteArray(coder, getCurrent());
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() throws IOException {
        try {
          readerClosedCount++;
          // Enforce the AutoCloseable contract. Close is not idempotent.
          assertThat(closed, is(false));
          if (throwOnClose) {
            throw new IOException(String.format("%s throws on close", TestUnboundedSource.this));
          }
        } finally {
          closed = true;
        }
      }
    }
  }

  private static class TestCheckpointMark implements CheckpointMark {
    final int index;
    private boolean finalized = false;
    private boolean decoded = false;

    private TestCheckpointMark(int index) {
      this.index = index;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      checkState(
          !finalized, "%s was finalized more than once", TestCheckpointMark.class.getSimpleName());
      checkState(
          !decoded,
          "%s was finalized after being decoded",
          TestCheckpointMark.class.getSimpleName());
      finalized = true;
    }

    boolean isFinalized() {
      return finalized;
    }

    public static class Coder extends AtomicCoder<TestCheckpointMark> {
      @Override
      public void encode(TestCheckpointMark value, OutputStream outStream) throws IOException {
        VarInt.encode(value.index, outStream);
      }

      @Override
      public TestCheckpointMark decode(InputStream inStream) throws IOException {
        TestCheckpointMark decoded = new TestCheckpointMark(VarInt.decodeInt(inStream));
        decoded.decoded = true;
        return decoded;
      }
    }
  }
}
