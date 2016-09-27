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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
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
/**
 * Tests for {@link UnboundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadEvaluatorFactoryTest {
  private PCollection<Long> longs;
  private TransformEvaluatorFactory factory;
  private EvaluationContext context;
  private UncommittedBundle<Long> output;

  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Before
  public void setup() {
    UnboundedSource<Long, ?> source =
        CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    longs = p.apply(Read.from(source));

    context = mock(EvaluationContext.class);
    factory = new UnboundedReadEvaluatorFactory(context);
    output = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(output);
  }

  @Test
  public void unboundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);

    TransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));
  }

  /**
   * Demonstrate that multiple sequential creations will produce additional elements if the source
   * can provide them.
   */
  @Test
  public void unboundedSourceInMemoryTransformEvaluatorMultipleSequentialCalls() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);

    TransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));

    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    TransformResult secondResult = secondEvaluator.finishBundle();
    assertThat(
        secondResult.getWatermarkHold(),
        Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(11L), tgw(12L), tgw(14L), tgw(18L), tgw(19L), tgw(17L), tgw(16L),
            tgw(15L), tgw(13L), tgw(10L)));
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
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);
    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);

    evaluator.finishBundle();
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(1L), tgw(2L), tgw(4L), tgw(3L), tgw(0L)));

    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator = factory.forApplication(sourceTransform, null);
    secondEvaluator.finishBundle();
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<Long>>emptyIterable());
  }

  @Test
  public void evaluatorClosesReaderAfterOutputCount() throws Exception {
    ContiguousSet<Long> elems = ContiguousSet.create(
        Range.closed(0L, 20L * UnboundedReadEvaluatorFactory.MAX_READER_REUSE_COUNT),
        DiscreteDomain.longs());
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), elems.toArray(new Long[0]));

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    for (int i = 0; i < UnboundedReadEvaluatorFactory.MAX_READER_REUSE_COUNT + 1; i++) {
      TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);
      evaluator.finishBundle();
    }
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(1));
  }

  @Test
  public void evaluatorReusesReaderBeforeCount() throws Exception {
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), 1L, 2L, 3L);

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(ImmutableList.copyOf(committed.getElements()), hasSize(3));
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(0));
    assertThat(TestUnboundedSource.readerAdvancedCount, equalTo(4));

    evaluator = factory.forApplication(sourceTransform, null);
    evaluator.finishBundle();
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(0));
    // Tried to advance again, even with no elements
    assertThat(TestUnboundedSource.readerAdvancedCount, equalTo(5));
  }

  @Test
  public void evaluatorNoElementsReusesReaderAlways() throws Exception {
    TestUnboundedSource<Long> source = new TestUnboundedSource<>(BigEndianLongCoder.of());

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    for (int i = 0; i < 2 * UnboundedReadEvaluatorFactory.MAX_READER_REUSE_COUNT; i++) {
      TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);
      evaluator.finishBundle();
    }
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(0));
    assertThat(TestUnboundedSource.readerAdvancedCount,
        equalTo(2 * UnboundedReadEvaluatorFactory.MAX_READER_REUSE_COUNT));
  }

  // TODO: Once the source is split into multiple sources before evaluating, this test will have to
  // be updated.
  /**
   * Demonstrate that only a single unfinished instance of TransformEvaluator can be created at a
   * time, with other calls returning an empty evaluator.
   */
  @Test
  public void unboundedSourceWithMultipleSimultaneousEvaluatorsIndependent() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);

    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);

    assertThat(secondEvaluator, nullValue());
    TransformResult result = evaluator.finishBundle();

    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));
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
        PipelineOptions options, TestCheckpointMark checkpointMark) {
      return new TestUnboundedReader(elems);
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

      public TestUnboundedReader(List<T> elems) {
        this.elems = elems;
        this.index = -1;
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
        return Instant.now();
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new TestCheckpointMark();
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
    @Override
    public void finalizeCheckpoint() throws IOException {}

    public static class Coder extends AtomicCoder<TestCheckpointMark> {
      @Override
      public void encode(
          TestCheckpointMark value,
          OutputStream outStream,
          org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {}

      @Override
      public TestCheckpointMark decode(
          InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        return new TestCheckpointMark();
      }
    }
  }
}
