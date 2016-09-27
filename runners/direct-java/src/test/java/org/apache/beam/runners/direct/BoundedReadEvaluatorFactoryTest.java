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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link BoundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class BoundedReadEvaluatorFactoryTest {
  private BoundedSource<Long> source;
  private PCollection<Long> longs;
  private TransformEvaluatorFactory factory;
  @Mock private EvaluationContext context;
  private BundleFactory bundleFactory;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    source = CountingSource.upTo(10L);
    TestPipeline p = TestPipeline.create();
    longs = p.apply(Read.from(source));

    factory = new BoundedReadEvaluatorFactory(context);
    bundleFactory = ImmutableListBundleFactory.create();
  }

  @Test
  public void boundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    UncommittedBundle<Long> output = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    TransformResult result = evaluator.finishBundle();
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(
        output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE).getElements(),
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  /**
   * Demonstrate that acquiring multiple {@link TransformEvaluator TransformEvaluators} for the same
   * {@link Bounded Read.Bounded} application with the same evaluation context only produces the
   * elements once.
   */
  @Test
  public void boundedSourceInMemoryTransformEvaluatorAfterFinishIsEmpty() throws Exception {
    UncommittedBundle<Long> output = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    TransformResult result = evaluator.finishBundle();
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    Iterable<? extends WindowedValue<Long>> outputElements =
        output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE).getElements();
    assertThat(
        outputElements,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));

    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    assertThat(secondEvaluator, nullValue());
  }

  /**
   * Demonstrates that acquiring multiple evaluators from the factory are independent, but
   * the elements in the source are only produced once.
   */
  @Test
  public void boundedSourceEvaluatorSimultaneousEvaluations() throws Exception {
    UncommittedBundle<Long> output = bundleFactory.createBundle(longs);
    UncommittedBundle<Long> secondOutput = bundleFactory.createBundle(longs);
    when(context.createBundle(longs)).thenReturn(output).thenReturn(secondOutput);

    // create both evaluators before finishing either.
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null);
    assertThat(secondEvaluator, nullValue());

    TransformResult result = evaluator.finishBundle();
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    Iterable<? extends WindowedValue<Long>> outputElements =
        output.commit(BoundedWindow.TIMESTAMP_MAX_VALUE).getElements();

    assertThat(
        outputElements,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
    assertThat(
        secondOutput.commit(BoundedWindow.TIMESTAMP_MAX_VALUE).getElements(), emptyIterable());
    assertThat(
        outputElements,
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  @Test
  public void boundedSourceEvaluatorClosesReader() throws Exception {
    TestSource<Long> source = new TestSource<>(BigEndianLongCoder.of(), 1L, 2L, 3L);

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(gw(2L), gw(3L), gw(1L)));
    assertThat(TestSource.readerClosed, is(true));
  }

  @Test
  public void boundedSourceEvaluatorNoElementsClosesReader() throws Exception {
    TestSource<Long> source = new TestSource<>(BigEndianLongCoder.of());

    TestPipeline p = TestPipeline.create();
    PCollection<Long> pcollection = p.apply(Read.from(source));
    AppliedPTransform<?, ?, ?> sourceTransform = pcollection.getProducingTransformInternal();

    UncommittedBundle<Long> output = bundleFactory.createBundle(pcollection);
    when(context.createBundle(pcollection)).thenReturn(output);

    TransformEvaluator<?> evaluator = factory.forApplication(sourceTransform, null);
    evaluator.finishBundle();
    CommittedBundle<Long> committed = output.commit(Instant.now());
    assertThat(committed.getElements(), emptyIterable());
    assertThat(TestSource.readerClosed, is(true));
  }

  private static class TestSource<T> extends BoundedSource<T> {
    private static boolean readerClosed;
    private final Coder<T> coder;
    private final T[] elems;

    public TestSource(Coder<T> coder, T... elems) {
      this.elems = elems;
      this.coder = coder;
      readerClosed = false;
    }

    @Override
    public List<? extends BoundedSource<T>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new TestReader<>(this, elems);
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }
  }

  private static class TestReader<T> extends BoundedReader<T> {
    private final BoundedSource<T> source;
    private final List<T> elems;
    private int index;

    public TestReader(BoundedSource<T> source, T... elems) {
      this.source = source;
      this.elems = Arrays.asList(elems);
      this.index = -1;
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (elems.size() > index + 1) {
        index++;
        return true;
      }
      return false;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return elems.get(index);
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
