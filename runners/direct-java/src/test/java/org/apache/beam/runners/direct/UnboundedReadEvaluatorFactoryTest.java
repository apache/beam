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

import static org.apache.beam.runners.direct.DirectGraphs.getProducer;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.NeverDeduplicator;
import org.apache.beam.runners.direct.UnboundedReadEvaluatorFactory.UnboundedSourceShard;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link UnboundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadEvaluatorFactoryTest {
  private PCollection<Long> longs;
  private UnboundedReadEvaluatorFactory factory;
  private EvaluationContext context;
  private UncommittedBundle<Long> output;

  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  private UnboundedSource<Long, ?> source;
  private DirectGraph graph;

  @Before
  public void setup() {
    source = CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    longs = p.apply(Read.from(source));

    context = mock(EvaluationContext.class);
    factory = new UnboundedReadEvaluatorFactory(context);
    output = bundleFactory.createBundle(longs);
    graph = DirectGraphs.getGraph(p);
    when(context.createBundle(longs)).thenReturn(output);
  }

  @Test
  public void generatesInitialSplits() throws Exception {
    when(context.createRootBundle()).thenAnswer(new Answer<UncommittedBundle<?>>() {
      @Override
      public UncommittedBundle<?> answer(InvocationOnMock invocation) throws Throwable {
        return bundleFactory.createRootBundle();
      }
    });

    int numSplits = 5;
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context)
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
      assertThat(shard.getWindows(), Matchers.<BoundedWindow>contains(GlobalWindow.INSTANCE));
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
        new UnboundedReadEvaluatorFactory.InputProvider(context)
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
    assertThat(
        residual.getTimestamp(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    UnboundedSourceShard<Long, ?> residualShard =
        (UnboundedSourceShard<Long, ?>) residual.getValue();
    assertThat(
        residualShard.getSource(),
        Matchers.<UnboundedSource<Long, ?>>equalTo(inputShard.getSource()));
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
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), outputs);
    source.dedupes = true;

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context)
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
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<Long>>emptyIterable());
  }

  @Test
  public void noElementsAvailableReaderIncludedInResidual() throws Exception {
    TestPipeline p = TestPipeline.create();
    // Read with a very slow rate so by the second read there are no more elements
    PCollection<Long> pcollection =
        p.apply(Read.from(new TestUnboundedSource<>(VarLongCoder.of(), 1L)));
    AppliedPTransform<?, ?, ?> sourceTransform = DirectGraphs.getProducer(pcollection);

    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    Collection<CommittedBundle<?>> initialInputs =
        new UnboundedReadEvaluatorFactory.InputProvider(context)
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
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<Long>>emptyIterable());

    // Test that even though the reader produced no outputs, there is still a residual shard with
    // the updated watermark.
    WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>> unprocessed =
        (WindowedValue<UnboundedSourceShard<Long, TestCheckpointMark>>)
            Iterables.getOnlyElement(secondResult.getUnprocessedElements());
    assertThat(
        unprocessed.getTimestamp(), Matchers.<ReadableInstant>greaterThan(residual.getTimestamp()));
    assertThat(unprocessed.getValue().getExistingReader(), not(nullValue()));
  }

  @Test
  public void evaluatorReusesReader() throws Exception {
    ContiguousSet<Long> elems = ContiguousSet.create(Range.closed(0L, 20L), DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]));

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    DirectGraph graph = DirectGraphs.getGraph(p);
    AppliedPTransform<?, ?, ?> sourceTransform =
        graph.getProducer(pcollection);

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
        new UnboundedReadEvaluatorFactory(context, 1.0 /* Always reuse */);
    new UnboundedReadEvaluatorFactory.InputProvider(context).getInitialInputs(sourceTransform, 1);
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

    assertThat(TestUnboundedSource.readerClosedCount, equalTo(0));
  }

  @Test
  public void evaluatorClosesReaderAndResumesFromCheckpoint() throws Exception {
    ContiguousSet<Long> elems = ContiguousSet.create(Range.closed(0L, 20L), DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]));

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform =
        DirectGraphs.getGraph(p).getProducer(pcollection);

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
        new UnboundedReadEvaluatorFactory(context, 0.0 /* never reuse */);
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
  }

  /**
   * A terse alias for producing timestamped longs in the {@link GlobalWindow}, where
   * the timestamp is the epoch offset by the value of the element.
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

    static int readerClosedCount;
    static int readerAdvancedCount;
    private final Coder<T> coder;
    private final List<T> elems;
    private boolean dedupes = false;

    public TestUnboundedSource(Coder<T> coder, T... elems) {
      readerAdvancedCount = 0;
      readerClosedCount = 0;
      this.coder = coder;
      this.elems = Arrays.asList(elems);
    }

    @Override
    public List<? extends UnboundedSource<T, TestCheckpointMark>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public UnboundedSource.UnboundedReader<T> createReader(
        PipelineOptions options, @Nullable TestCheckpointMark checkpointMark) {
      return new TestUnboundedReader(elems, checkpointMark == null ? -1 : checkpointMark.index);
    }

    @Override
    @Nullable
    public Coder<TestCheckpointMark> getCheckpointMarkCoder() {
      return new TestCheckpointMark.Coder();
    }

    @Override
    public boolean requiresDeduping() {
      return dedupes;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }

    private class TestUnboundedReader extends UnboundedReader<T> {
      private final List<T> elems;
      private int index;

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
        return new Instant(index + getWatermarkCalls);
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new TestCheckpointMark(index);
      }

      @Override
      public UnboundedSource<T, ?> getCurrentSource() {
        TestUnboundedSource<T> source = TestUnboundedSource.this;
        return source;
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
        readerClosedCount++;
      }
    }
  }

  private static class TestCheckpointMark implements CheckpointMark {
    final int index;

    private TestCheckpointMark(int index) {
      this.index = index;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {}

    public static class Coder extends AtomicCoder<TestCheckpointMark> {
      @Override
      public void encode(
          TestCheckpointMark value,
          OutputStream outStream,
          org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        VarInt.encode(value.index, outStream);
      }

      @Override
      public TestCheckpointMark decode(
          InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        return new TestCheckpointMark(VarInt.decodeInt(inStream));
      }
    }
  }
}
