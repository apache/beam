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
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;
/**
 * Tests for {@link UnboundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadEvaluatorFactoryTest {
  private PCollection<Long> longs;
  private TransformEvaluatorFactory factory;
  private InProcessEvaluationContext context;
  private UncommittedBundle<Long> output;

  private BundleFactory bundleFactory = InProcessBundleFactory.create();

  @Before
  public void setup() {
    UnboundedSource<Long, ?> source =
        CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    longs = p.apply(Read.from(source));

    factory = new UnboundedReadEvaluatorFactory();
    context = mock(InProcessEvaluationContext.class);
    output = bundleFactory.createRootBundle(longs);
    when(context.createRootBundle(longs)).thenReturn(output);
  }

  @Test
  public void unboundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    InProcessTransformResult result = evaluator.finishBundle();
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
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));

    UncommittedBundle<Long> secondOutput = bundleFactory.createRootBundle(longs);
    when(context.createRootBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();
    assertThat(
        secondResult.getWatermarkHold(),
        Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(11L), tgw(12L), tgw(14L), tgw(18L), tgw(19L), tgw(17L), tgw(16L),
            tgw(15L), tgw(13L), tgw(10L)));
  }

  @Test
  public void evaluatorClosesReader() throws Exception {
    TestUnboundedSource<Long> source =
        new TestUnboundedSource<>(BigEndianLongCoder.of(), 1L, 2L, 3L);

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createRootBundle(pcollection);
    when(context.createRootBundle(pcollection)).thenReturn(output);

    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null, context);
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(ImmutableList.copyOf(committed.getElements()), hasSize(3));
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(1));
  }

  @Test
  public void evaluatorNoElementsClosesReader() throws Exception {
    TestUnboundedSource<Long> source = new TestUnboundedSource<>(BigEndianLongCoder.of());

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createRootBundle(pcollection);
    when(context.createRootBundle(pcollection)).thenReturn(output);

    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null, context);
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(committed.getElements(), emptyIterable());
    assertThat(TestUnboundedSource.readerClosedCount, equalTo(1));
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
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    assertThat(secondEvaluator, nullValue());
    InProcessTransformResult result = evaluator.finishBundle();

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
    private final Coder<T> coder;
    private final List<T> elems;

    public TestUnboundedSource(Coder<T> coder, T... elems) {
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
        return Instant.now();
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
