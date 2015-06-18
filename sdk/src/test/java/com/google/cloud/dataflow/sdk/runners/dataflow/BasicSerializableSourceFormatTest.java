/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.dataflow;

import static com.google.cloud.dataflow.sdk.io.SourceTestUtils.readFromSource;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.splitRequestAtFraction;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceOperationRequestToSourceOperationRequest;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.dictionaryToCloudSource;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceOperationResponseToCloudSourceOperationResponse;
import static com.google.cloud.dataflow.sdk.util.Structs.getDictionary;
import static com.google.cloud.dataflow.sdk.util.Structs.getObject;
import static com.google.cloud.dataflow.sdk.util.WindowedValue.valueInGlobalWindow;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.DerivedSource;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.api.services.dataflow.model.Step;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.worker.ReaderFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Sample;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CloudSourceUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Tests for {@code BasicSerializableSourceFormat}.
 */
@RunWith(JUnit4.class)
public class BasicSerializableSourceFormatTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  static class TestIO {
    public static Read fromRange(int from, int to) {
      return new Read(from, to, false);
    }

    static class Read extends BoundedSource<Integer> {
      private static final long serialVersionUID = 0;

      final int from;
      final int to;
      final boolean produceTimestamps;

      Read(int from, int to, boolean produceTimestamps) {
        this.from = from;
        this.to = to;
        this.produceTimestamps = produceTimestamps;
      }

      public Read withTimestampsMillis() {
        return new Read(from, to, true);
      }

      @Override
      public List<Read> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
          throws Exception {
        List<Read> res = new ArrayList<>();
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        float step = 1.0f * (to - from) / dataflowOptions.getNumWorkers();
        for (int i = 0; i < dataflowOptions.getNumWorkers(); ++i) {
          res.add(new Read(
              Math.round(from + i * step), Math.round(from + (i + 1) * step),
              produceTimestamps));
        }
        return res;
      }

      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 8 * (to - from);
      }

      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return true;
      }

      @Override
      public BoundedReader<Integer> createReader(
          PipelineOptions options, @Nullable ExecutionContext executionContext) throws IOException {
        return new RangeReader(this);
      }

      @Override
      public void validate() {}

      @Override
      public String toString() {
        return "[" + from + ", " + to + ")";
      }

      @Override
      public Coder<Integer> getDefaultOutputCoder() {
        return BigEndianIntegerCoder.of();
      }

      private static class RangeReader extends AbstractBoundedReader<Integer> {
        // To verify that BasicSerializableSourceFormat calls our methods according to protocol.
        enum State {
          UNSTARTED,
          STARTED,
          FINISHED
        }
        private Read source;
        private int current = -1;
        private State state = State.UNSTARTED;

        public RangeReader(Read source) {
          this.source = source;
        }

        @Override
        public boolean start() throws IOException {
          Preconditions.checkState(state == State.UNSTARTED);
          state = State.STARTED;
          current = source.from;
          return (current < source.to);
        }

        @Override
        public boolean advance() throws IOException {
          Preconditions.checkState(state == State.STARTED);
          if (current == source.to - 1) {
            state = State.FINISHED;
            return false;
          }
          current++;
          return true;
        }

        @Override
        public Integer getCurrent() {
          Preconditions.checkState(state == State.STARTED);
          return current;
        }

        @Override
        public Instant getCurrentTimestamp() {
          return source.produceTimestamps
              ? new Instant(current /* as millis */) : BoundedWindow.TIMESTAMP_MIN_VALUE;
        }

        @Override
        public void close() throws IOException {
          Preconditions.checkState(state == State.STARTED || state == State.FINISHED);
          state = State.FINISHED;
        }

        @Override
        public Read getCurrentSource() {
          return source;
        }

        @Override
        public Read splitAtFraction(double fraction) {
          int proposedIndex = (int) (source.from + fraction * (source.to - source.from));
          if (proposedIndex <= current) {
            return null;
          }
          Read primary = new Read(source.from, proposedIndex, source.produceTimestamps);
          Read residual = new Read(proposedIndex, source.to, source.produceTimestamps);
          this.source = primary;
          return residual;
        }

        @Override
        public Double getFractionConsumed() {
          return (current == -1)
              ? 0.0
              : (1.0 * (1 + current - source.from) / (source.to - source.from));
        }
      }
    }
  }

  @Test
  public void testSplitAndReadBundlesBack() throws Exception {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setNumWorkers(5);
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(TestIO.fromRange(10, 20), options);
    List<WindowedValue<Integer>> elems = CloudSourceUtils.readElemsFromSource(options, source);
    assertEquals(10, elems.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals(valueInGlobalWindow(10 + i), elems.get(i));
    }
    SourceSplitResponse response = performSplit(source, options);
    assertEquals("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED", response.getOutcome());
    List<DerivedSource> bundles = response.getBundles();
    assertEquals(5, bundles.size());
    for (int i = 0; i < 5; ++i) {
      DerivedSource bundle = bundles.get(i);
      assertEquals("SOURCE_DERIVATION_MODE_INDEPENDENT", bundle.getDerivationMode());
      com.google.api.services.dataflow.model.Source bundleSource = bundle.getSource();
      assertTrue(bundleSource.getDoesNotNeedSplitting());
      bundleSource.setCodec(source.getCodec());
      List<WindowedValue<Integer>> xs = CloudSourceUtils.readElemsFromSource(options, bundleSource);
      assertThat(xs, contains(valueInGlobalWindow(10 + 2 * i), valueInGlobalWindow(11 + 2 * i)));
    }
  }

  @Test
  public void testDirectPipelineWithoutTimestamps() throws Exception {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> sum = p
        .apply(Read.from(TestIO.fromRange(10, 20)))
        .apply(Sum.integersGlobally())
        .apply(Sample.<Integer>any(1));

    DataflowAssert.thatSingleton(sum).isEqualTo(145);
  }

  @Test
  public void testDirectPipelineWithTimestamps() throws Exception {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> sums =
        p.apply(Read.from(TestIO.fromRange(10, 20).withTimestampsMillis()))
         .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(3))))
         .apply(Sum.integersGlobally().withoutDefaults());
    // Should group into [10 11] [12 13 14] [15 16 17] [18 19].
    DataflowAssert.that(sums).containsInAnyOrder(21, 37, 39, 48);
  }

  @Test
  public void testRangeProgressAndSplitAtFraction() throws Exception {
    // Show basic usage of getFractionConsumed and splitAtFraction.
    // This test only tests TestIO itself, not BasicSerializableSourceFormat.

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    TestIO.Read source = TestIO.fromRange(10, 20);
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options, null)) {
      assertEquals(0, reader.getFractionConsumed().intValue());
      assertTrue(reader.start());
      assertEquals(0.1, reader.getFractionConsumed(), 1e-6);
      assertTrue(reader.advance());
      assertEquals(0.2, reader.getFractionConsumed(), 1e-6);
      // Already past 0.0 and 0.1.
      assertNull(reader.splitAtFraction(0.0));
      assertNull(reader.splitAtFraction(0.1));

      {
        TestIO.Read residual = (TestIO.Read) reader.splitAtFraction(0.5);
        assertNotNull(residual);
        TestIO.Read primary = (TestIO.Read) reader.getCurrentSource();
        assertThat(readFromSource(primary, options), contains(10, 11, 12, 13, 14));
        assertThat(readFromSource(residual, options), contains(15, 16, 17, 18, 19));
      }

      // Range is now [10, 15) and we are at 12.
      {
        TestIO.Read residual = (TestIO.Read) reader.splitAtFraction(0.8); // give up 14.
        assertNotNull(residual);
        TestIO.Read primary = (TestIO.Read) reader.getCurrentSource();
        assertThat(readFromSource(primary, options), contains(10, 11, 12, 13));
        assertThat(readFromSource(residual, options), contains(14));
      }

      assertTrue(reader.advance());
      assertEquals(12, reader.getCurrent().intValue());
      assertTrue(reader.advance());
      assertEquals(13, reader.getCurrent().intValue());
      assertFalse(reader.advance());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProgressAndSourceSplitTranslation() throws Exception {
    // Same as previous test, but now using BasicSerializableSourceFormat wrappers.
    // We know that the underlying reader behaves correctly (because of the previous test),
    // now check that we are wrapping it correctly.
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);
    Reader<WindowedValue<Integer>> reader = ReaderFactory.create(
        options, translateIOToCloudSource(TestIO.fromRange(10, 20), options), null);
    try (Reader.ReaderIterator<WindowedValue<Integer>> iterator = reader.iterator()) {
      assertTrue(iterator.hasNext());
      assertEquals(
          0, readerProgressToCloudProgress(iterator.getProgress()).getPercentComplete().intValue());
      assertEquals(valueInGlobalWindow(10), iterator.next());
      assertEquals(
          0.1,
          readerProgressToCloudProgress(iterator.getProgress()).getPercentComplete().doubleValue(),
          1e-6);
      assertEquals(valueInGlobalWindow(11), iterator.next());
      assertEquals(
          0.2,
          readerProgressToCloudProgress(iterator.getProgress()).getPercentComplete().doubleValue(),
          1e-6);
      assertEquals(valueInGlobalWindow(12), iterator.next());

      assertNull(iterator.requestDynamicSplit(splitRequestAtFraction(0)));
      assertNull(iterator.requestDynamicSplit(splitRequestAtFraction(0.1f)));
      BasicSerializableSourceFormat.BoundedSourceSplit<Integer> sourceSplit =
          (BasicSerializableSourceFormat.BoundedSourceSplit<Integer>)
              iterator.requestDynamicSplit(splitRequestAtFraction(0.5f));
      assertNotNull(sourceSplit);
      assertThat(readFromSource(sourceSplit.primary, options), contains(10, 11, 12, 13, 14));
      assertThat(readFromSource(sourceSplit.residual, options), contains(15, 16, 17, 18, 19));

      sourceSplit =
          (BasicSerializableSourceFormat.BoundedSourceSplit<Integer>)
              iterator.requestDynamicSplit(splitRequestAtFraction(0.8f));
      assertNotNull(sourceSplit);
      assertThat(readFromSource(sourceSplit.primary, options), contains(10, 11, 12, 13));
      assertThat(readFromSource(sourceSplit.residual, options), contains(14));

      assertTrue(iterator.hasNext());
      assertEquals(valueInGlobalWindow(13), iterator.next());
      assertFalse(iterator.hasNext());
    }
  }

  /**
   * A source that cannot do anything. Intended to be overridden for testing of individual methods.
   */
  private static class MockSource extends BoundedSource<Integer> {
    private static final long serialVersionUID = -5041539913488064889L;

    @Override
    public List<? extends BoundedSource<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Arrays.asList(this);
    }

    @Override
    public void validate() { }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      return false;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options, ExecutionContext context)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "<unknown>";
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return BigEndianIntegerCoder.of();
    }
  }

  private static class SourceProducingInvalidSplits extends MockSource {
    private static final long serialVersionUID = -1731497848893255523L;

    private String description;
    private String errorMessage;

    private SourceProducingInvalidSplits(String description, String errorMessage) {
      this.description = description;
      this.errorMessage = errorMessage;
    }

    @Override
    public List<? extends BoundedSource<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      Preconditions.checkState(errorMessage == null, "Unexpected invalid source");
      return Arrays.asList(
          new SourceProducingInvalidSplits("goodBundle", null),
          new SourceProducingInvalidSplits("badBundle", "intentionally invalid"));
    }

    @Override
    public void validate() {
      Preconditions.checkState(errorMessage == null, errorMessage);
    }

    @Override
    public String toString() {
      return description;
    }
  }

  @Test
  public void testSplittingProducedInvalidSource() throws Exception {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    com.google.api.services.dataflow.model.Source cloudSource =
        translateIOToCloudSource(new SourceProducingInvalidSplits("original", null), options);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(allOf(
        containsString("Splitting a valid source produced an invalid bundle"),
        containsString("original"),
        containsString("badBundle")));
    expectedException.expectCause(hasMessage(containsString("intentionally invalid")));
    performSplit(cloudSource, options);
  }

  private static class FailingReader implements BoundedSource.BoundedReader<Integer> {
    private BoundedSource<Integer> source;

    private FailingReader(BoundedSource<Integer> source) {
      this.source = source;
    }

    @Override
    public BoundedSource<Integer> getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      throw new IOException("Intentional error");
    }

    @Override
    public boolean advance() throws IOException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      throw new IllegalStateException("Should have failed in start()");
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Double getFractionConsumed() {
      return null;
    }

    @Override
    public BoundedSource<Integer> splitAtFraction(double fraction) {
      return null;
    }
  }

  private static class SourceProducingFailingReader extends MockSource {
    private static final long serialVersionUID = -1288303253742972653L;

    @Override
    public BoundedReader<Integer> createReader(
        PipelineOptions options, @Nullable ExecutionContext executionContext) throws IOException {
      return new FailingReader(this);
    }

    @Override
    public String toString() {
      return "Some description";
    }
  }

  @Test
  public void testFailureToStartReadingIncludesSourceDetails() throws Exception {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    com.google.api.services.dataflow.model.Source source =
        translateIOToCloudSource(new SourceProducingFailingReader(), options);
    // Unfortunately Hamcrest doesn't have a matcher that can match on the exception's
    // printStackTrace(), however we just want to verify that the error and source description
    // would be contained in the exception *somewhere*, not necessarily in the top-level
    // Exception object. So instead we use Throwables.getStackTraceAsString and match on that.
    try {
      CloudSourceUtils.readElemsFromSource(options, source);
      fail("Expected to fail");
    } catch (Exception e) {
      assertThat(
          getStackTraceAsString(e),
          allOf(containsString("Intentional error"), containsString("Some description")));
    }
  }

  private static com.google.api.services.dataflow.model.Source translateIOToCloudSource(
      BoundedSource<?> io, DataflowPipelineOptions options) throws Exception {
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline p = Pipeline.create(options);
    p.begin().apply(Read.from(io));

    Job workflow = translator.translate(p, new ArrayList<DataflowPackage>()).getJob();
    Step step = workflow.getSteps().get(0);

    return stepToCloudSource(step);
  }

  private static com.google.api.services.dataflow.model.Source stepToCloudSource(Step step)
      throws Exception {
    com.google.api.services.dataflow.model.Source res = dictionaryToCloudSource(
        getDictionary(step.getProperties(), PropertyNames.SOURCE_STEP_INPUT));
    // Encoding is specified in the step, not in the source itself.  This is
    // normal: incoming Dataflow API Source objects in map tasks will have the
    // encoding filled in from the step's output encoding.
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> outputInfo =
        (List<Map<String, Object>>) step.getProperties().get(PropertyNames.OUTPUT_INFO);

    CloudObject encoding = CloudObject.fromSpec(getObject(outputInfo.get(0),
        PropertyNames.ENCODING));
    res.setCodec(encoding);
    return res;
  }

  private static SourceSplitResponse performSplit(
      com.google.api.services.dataflow.model.Source source, PipelineOptions options)
      throws Exception {
    SourceSplitRequest splitRequest = new SourceSplitRequest();
    splitRequest.setSource(source);
    SourceOperationRequest request = new SourceOperationRequest();
    request.setSplit(splitRequest);
    SourceFormat.OperationRequest request1 =
        cloudSourceOperationRequestToSourceOperationRequest(request);
    SourceFormat.OperationResponse response =
        new BasicSerializableSourceFormat(options).performSourceOperation(request1);
    return sourceOperationResponseToCloudSourceOperationResponse(response).getSplit();
  }
}
