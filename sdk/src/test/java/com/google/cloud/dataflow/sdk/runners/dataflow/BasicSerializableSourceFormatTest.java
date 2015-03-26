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

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceOperationRequestToSourceOperationRequest;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.dictionaryToCloudSource;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceOperationResponseToCloudSourceOperationResponse;
import static com.google.cloud.dataflow.sdk.util.Structs.getDictionary;
import static com.google.cloud.dataflow.sdk.util.Structs.getObject;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.api.services.dataflow.model.SourceSplitShard;
import com.google.api.services.dataflow.model.Step;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.ReadSource;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CloudSourceUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;
import com.google.common.base.Preconditions;

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
      return new Read(from, to);
    }

    static class Read extends Source<Integer> {
      private static final long serialVersionUID = 0;

      final int from;
      final int to;

      Read(int from, int to) {
        this.from = from;
        this.to = to;
      }

      @Override
      public List<Read> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
          throws Exception {
        List<Read> res = new ArrayList<>();
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        float step = 1.0f * (to - from) / dataflowOptions.getNumWorkers();
        for (int i = 0; i < dataflowOptions.getNumWorkers(); ++i) {
          res.add(new Read(Math.round(from + i * step), Math.round(from + (i + 1) * step)));
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
      public Reader<Integer> createReader(
          PipelineOptions options, Coder<Integer> coder,
          @Nullable ExecutionContext executionContext) throws IOException {
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

      private class RangeReader implements Reader<Integer> {
        private int current;

        public RangeReader(Read source) {
          this.current = source.from;
        }

        @Override
        public boolean start() throws IOException {
          return (current < to);
        }

        @Override
        public boolean advance() throws IOException {
          current++;
          return (current < to);
        }

        @Override
        public Integer getCurrent() {
          return current;
        }

        @Override
        public void close() throws IOException {
          // Nothing
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
    List<Integer> elems = CloudSourceUtils.readElemsFromSource(options, source);
    assertEquals(10, elems.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals(Integer.valueOf(10 + i), elems.get(i));
    }
    SourceSplitResponse response = performSplit(source, options);
    assertEquals("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED", response.getOutcome());
    List<SourceSplitShard> bundles = response.getShards();
    assertEquals(5, bundles.size());
    for (int i = 0; i < 5; ++i) {
      SourceSplitShard bundle = bundles.get(i);
      assertEquals("SOURCE_DERIVATION_MODE_INDEPENDENT", bundle.getDerivationMode());
      com.google.api.services.dataflow.model.Source bundleSource = bundle.getSource();
      assertTrue(bundleSource.getDoesNotNeedSplitting());
      bundleSource.setCodec(source.getCodec());
      List<Integer> xs = CloudSourceUtils.readElemsFromSource(options, bundleSource);
      assertThat(xs, contains(10 + 2 * i, 11 + 2 * i));
    }
  }

  /**
   * A source that cannot do anything. Intended to be overridden for testing of individual methods.
   */
  private static class MockSource extends Source<Integer> {
    private static final long serialVersionUID = -5041539913488064889L;

    @Override
    public List<? extends Source<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Arrays.asList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0L;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public void validate() { }

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
    public List<? extends Source<Integer>> splitIntoBundles(
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

  private static class FailingReader implements Source.Reader<Integer> {
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
    public void close() throws IOException { }
  }

  private static class SourceProducingFailingReader extends MockSource {
    private static final long serialVersionUID = -1288303253742972653L;

    @Override
    public Reader<Integer> createReader(
        PipelineOptions options, Coder<Integer> coder, @Nullable ExecutionContext executionContext)
        throws IOException {
      return new FailingReader();
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
      Source<?> io, DataflowPipelineOptions options) throws Exception {
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    Pipeline p = Pipeline.create(options);
    p.begin().apply(ReadSource.from(io));

    Job workflow = translator.translate(p, new ArrayList<DataflowPackage>());
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
    CloudObject encoding = CloudObject.fromSpec(getObject(
        // TODO: This should be done via a Structs accessor.
        ((List<Map<String, Object>>) step.getProperties().get(PropertyNames.OUTPUT_INFO)).get(0),
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
