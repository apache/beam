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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.direct.BoundedReadEvaluatorFactory.BoundedSourceShard;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.OffsetBasedSource.OffsetBasedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BoundedReadEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class BoundedReadEvaluatorFactoryTest {
  private BoundedSource<Long> source;
  private PCollection<Long> longs;
  private BoundedReadEvaluatorFactory factory;
  @Mock private EvaluationContext context;
  private BundleFactory bundleFactory;
  private AppliedPTransform<?, ?, ?> longsProducer;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private PipelineOptions options;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    source = CountingSource.upTo(10L);
    longs = p.apply(Read.from(source));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    factory =
        new BoundedReadEvaluatorFactory(
            context, p.getOptions(), Long.MAX_VALUE /* minimum size for dynamic splits */);
    bundleFactory = ImmutableListBundleFactory.create();
    longsProducer = DirectGraphs.getProducer(longs);
  }

  @Test
  public void boundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    UncommittedBundle<Long> outputBundle = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(outputBundle);

    Collection<CommittedBundle<?>> initialInputs =
        new BoundedReadEvaluatorFactory.InputProvider(context, options)
            .getInitialInputs(longsProducer, 1);
    List<WindowedValue<?>> outputs = new ArrayList<>();
    for (CommittedBundle<?> shardBundle : initialInputs) {
      TransformEvaluator<?> evaluator = factory.forApplication(longsProducer, null);
      for (WindowedValue<?> shard : shardBundle.getElements()) {
        evaluator.processElement((WindowedValue) shard);
      }
      TransformResult<?> result = evaluator.finishBundle();
      assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
      assertThat(
          Iterables.size(result.getOutputBundles()),
          equalTo(Iterables.size(shardBundle.getElements())));
      for (UncommittedBundle<?> output : result.getOutputBundles()) {
        CommittedBundle<?> committed = output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
        for (WindowedValue<?> val : committed.getElements()) {
          outputs.add(val);
        }
      }
    }

    assertThat(
        outputs,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  @Test
  public void boundedSourceEvaluatorProducesDynamicSplits() throws Exception {
    BoundedReadEvaluatorFactory factory = new BoundedReadEvaluatorFactory(context, options, 0L);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    int numElements = 10;
    Long[] elems = new Long[numElements];
    for (int i = 0; i < numElements; i++) {
      elems[i] = (long) i;
    }
    PCollection<Long> read = p.apply(Read.from(new TestSource<>(VarLongCoder.of(), 5, elems)));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    AppliedPTransform<?, ?, ?> transform = DirectGraphs.getProducer(read);
    Collection<CommittedBundle<?>> unreadInputs =
        new BoundedReadEvaluatorFactory.InputProvider(context, options)
            .getInitialInputs(transform, 1);

    Collection<WindowedValue<?>> outputs = new ArrayList<>();
    int numIterations = 0;
    while (!unreadInputs.isEmpty()) {
      numIterations++;
      UncommittedBundle<Long> outputBundle = bundleFactory.createBundle(read);
      when(context.createBundle(read)).thenReturn(outputBundle);

      Collection<CommittedBundle<?>> newUnreadInputs = new ArrayList<>();
      for (CommittedBundle<?> shardBundle : unreadInputs) {
        TransformEvaluator<Long> evaluator = factory.forApplication(transform, null);
        for (WindowedValue<?> shard : shardBundle.getElements()) {
          evaluator.processElement((WindowedValue) shard);
        }
        TransformResult<Long> result = evaluator.finishBundle();
        assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
        assertThat(
            Iterables.size(result.getOutputBundles()),
            equalTo(Iterables.size(shardBundle.getElements())));
        for (UncommittedBundle<?> output : result.getOutputBundles()) {
          CommittedBundle<?> committed = output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
          for (WindowedValue<?> val : committed.getElements()) {
            outputs.add(val);
          }
        }
        if (!Iterables.isEmpty(result.getUnprocessedElements())) {
          newUnreadInputs.add(shardBundle.withElements((Iterable) result.getUnprocessedElements()));
        }
      }
      unreadInputs = newUnreadInputs;
    }

    assertThat(numIterations, greaterThan(1));
    WindowedValue[] expectedValues = new WindowedValue[numElements];
    for (long i = 0L; i < numElements; i++) {
      expectedValues[(int) i] = gw(i);
    }
    assertThat(outputs, Matchers.<WindowedValue<?>>containsInAnyOrder(expectedValues));
  }

  @Test
  public void boundedSourceEvaluatorDynamicSplitsUnsplittable() throws Exception {
    BoundedReadEvaluatorFactory factory = new BoundedReadEvaluatorFactory(context, options, 0L);

    PCollection<Long> read =
        p.apply(Read.from(SourceTestUtils.toUnsplittableSource(CountingSource.upTo(10L))));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(p);
    AppliedPTransform<?, ?, ?> transform = DirectGraphs.getProducer(read);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new BoundedReadEvaluatorFactory.InputProvider(context, options)
            .getInitialInputs(transform, 1);

    UncommittedBundle<Long> outputBundle = bundleFactory.createBundle(read);
    when(context.createBundle(read)).thenReturn(outputBundle);
    List<WindowedValue<?>> outputs = new ArrayList<>();
    for (CommittedBundle<?> shardBundle : initialInputs) {
      TransformEvaluator<?> evaluator = factory.forApplication(transform, null);
      for (WindowedValue<?> shard : shardBundle.getElements()) {
        evaluator.processElement((WindowedValue) shard);
      }
      TransformResult<?> result = evaluator.finishBundle();
      assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
      assertThat(
          Iterables.size(result.getOutputBundles()),
          equalTo(Iterables.size(shardBundle.getElements())));
      for (UncommittedBundle<?> output : result.getOutputBundles()) {
        CommittedBundle<?> committed = output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
        for (WindowedValue<?> val : committed.getElements()) {
          outputs.add(val);
        }
      }
    }

    assertThat(
        outputs,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  @Test
  public void getInitialInputsSplitsIntoBundles() throws Exception {
    when(context.createRootBundle()).thenAnswer(invocation -> bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new BoundedReadEvaluatorFactory.InputProvider(context, options)
            .getInitialInputs(longsProducer, 3);

    assertThat(initialInputs, hasSize(allOf(greaterThanOrEqualTo(3), lessThanOrEqualTo(4))));

    Collection<BoundedSource<Long>> sources = new ArrayList<>();
    for (CommittedBundle<?> initialInput : initialInputs) {
      Iterable<WindowedValue<BoundedSourceShard<Long>>> shards =
          (Iterable) initialInput.getElements();
      WindowedValue<BoundedSourceShard<Long>> shard = Iterables.getOnlyElement(shards);
      assertThat(shard.getWindows(), Matchers.contains(GlobalWindow.INSTANCE));
      assertThat(shard.getTimestamp(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
      sources.add(shard.getValue().getSource());
    }

    SourceTestUtils.assertSourcesEqualReferenceSource(
        source, (List<? extends BoundedSource<Long>>) sources, PipelineOptionsFactory.create());
  }

  @Test
  public void boundedSourceInMemoryTransformEvaluatorShardsOfSource() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Long>> splits =
        source.split(source.getEstimatedSizeBytes(options) / 2, options);

    UncommittedBundle<BoundedSourceShard<Long>> rootBundle = bundleFactory.createRootBundle();
    for (BoundedSource<Long> split : splits) {
      BoundedSourceShard<Long> shard = BoundedSourceShard.of(split);
      rootBundle.add(WindowedValue.valueInGlobalWindow(shard));
    }
    CommittedBundle<BoundedSourceShard<Long>> shards = rootBundle.commit(Instant.now());

    TransformEvaluator<BoundedSourceShard<Long>> evaluator =
        factory.forApplication(longsProducer, shards);
    for (WindowedValue<BoundedSourceShard<Long>> shard : shards.getElements()) {
      UncommittedBundle<Long> outputBundle = bundleFactory.createBundle(longs);
      when(context.createBundle(longs)).thenReturn(outputBundle);
      evaluator.processElement(shard);
    }
    TransformResult<?> result = evaluator.finishBundle();
    assertThat(Iterables.size(result.getOutputBundles()), equalTo(splits.size()));

    List<WindowedValue<?>> outputElems = new ArrayList<>();
    for (UncommittedBundle<?> outputBundle : result.getOutputBundles()) {
      CommittedBundle<?> outputs = outputBundle.commit(Instant.now());
      for (WindowedValue<?> outputElem : outputs.getElements()) {
        outputElems.add(outputElem);
      }
    }
    assertThat(
        outputElems,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  @Test
  public void boundedSourceEvaluatorClosesReader() throws Exception {
    TestSource<Long> source = new TestSource<>(BigEndianLongCoder.of(), 1L, 2L, 3L);
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = DirectGraphs.getProducer(pcollection);

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    TransformEvaluator<BoundedSourceShard<Long>> evaluator =
        factory.forApplication(
            sourceTransform, bundleFactory.createRootBundle().commit(Instant.now()));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(BoundedSourceShard.of(source)));
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(gw(2L), gw(3L), gw(1L)));
    assertThat(TestSource.readerClosed, is(true));
  }

  @Test
  public void boundedSourceEvaluatorNoElementsClosesReader() throws Exception {
    TestSource<Long> source = new TestSource<>(BigEndianLongCoder.of());

    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = DirectGraphs.getProducer(pcollection);

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    TransformEvaluator<BoundedSourceShard<Long>> evaluator =
        factory.forApplication(
            sourceTransform, bundleFactory.createRootBundle().commit(Instant.now()));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(BoundedSourceShard.of(source)));
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(committed.getElements(), emptyIterable());
    assertThat(TestSource.readerClosed, is(true));
  }

  @Test
  public void cleanupShutsDownExecutor() {
    factory.cleanup();
    assertThat(factory.executor.isShutdown(), is(true));
  }

  private static class TestSource<T> extends OffsetBasedSource<T> {
    private static boolean readerClosed;
    private final Coder<T> coder;
    private final T[] elems;
    private final int firstSplitIndex;

    private transient CountDownLatch subrangesCompleted;

    public TestSource(Coder<T> coder, T... elems) {
      this(coder, elems.length, elems);
    }

    public TestSource(Coder<T> coder, int firstSplitIndex, T... elems) {
      super(0L, elems.length, 1L);
      this.elems = elems;
      this.coder = coder;
      this.firstSplitIndex = firstSplitIndex;
      readerClosed = false;

      subrangesCompleted = new CountDownLatch(2);
    }

    @Override
    public List<? extends OffsetBasedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return elems.length;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      subrangesCompleted = new CountDownLatch(2);
      return new TestReader<>(this, firstSplitIndex, subrangesCompleted);
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) throws Exception {
      return elems.length;
    }

    @Override
    public OffsetBasedSource<T> createSourceForSubrange(long start, long end) {
      subrangesCompleted.countDown();
      return new TestSource<>(coder, Arrays.copyOfRange(elems, (int) start, (int) end));
    }

    @Override
    public Coder<T> getOutputCoder() {
      return coder;
    }
  }

  private static class TestReader<T> extends OffsetBasedReader<T> {
    private final Source<T> initialSource;
    private final int sleepIndex;
    private final CountDownLatch dynamicallySplit;

    private int index;

    TestReader(OffsetBasedSource<T> source, int sleepIndex, CountDownLatch dynamicallySplit) {
      super(source);
      this.initialSource = source;
      this.sleepIndex = sleepIndex;
      this.dynamicallySplit = dynamicallySplit;
      this.index = -1;
    }

    @Override
    public TestSource<T> getCurrentSource() {
      return (TestSource<T>) super.getCurrentSource();
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      return (long) index;
    }

    @Override
    public boolean startImpl() throws IOException {
      return advanceImpl();
    }

    @Override
    public boolean advanceImpl() throws IOException {
      // Sleep before the sleep/split index is claimed so long as it will be claimed
      if (index + 1 == sleepIndex && sleepIndex < getCurrentSource().elems.length) {
        try {
          dynamicallySplit.await();
          while (initialSource.equals(getCurrentSource())) {
            // Spin until the current source is updated
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      if (getCurrentSource().elems.length > index + 1) {
        index++;
        return true;
      }
      return false;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return getCurrentSource().elems[index];
    }

    @Override
    public void close() throws IOException {
      TestSource.readerClosed = true;
    }
  }

  private static WindowedValue<Long> gw(Long elem) {
    return WindowedValue.valueInGlobalWindow(elem);
  }
}
