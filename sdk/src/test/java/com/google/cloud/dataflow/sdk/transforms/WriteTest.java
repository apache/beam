/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest.TestPipelineOptions;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for the Write PTransform.
 */
@RunWith(JUnit4.class)
public class WriteTest {
  // Static store that can be accessed within the writer
  static List<String> sinkContents = new ArrayList<>();

  /**
   * Test a Write transform with a PCollection of elements.
   */
  @Test
  public void testWrite() {
    List<String> inputs = Arrays.asList("Critical canary", "Apprehensive eagle",
        "Intimidating pigeon", "Pedantic gull", "Frisky finch");
    runWrite(inputs, /* not windowed */ false);
  }

  /**
   * Test a Write transform with an empty PCollection.
   */
  @Test
  public void testWriteWithEmptyPCollection() {
    List<String> inputs = new ArrayList<>();
    runWrite(inputs, /* not windowed */ false);
  }

  /**
   * Test a Write with a windowed PCollection.
   */
  @Test
  public void testWriteWindowed() {
    List<String> inputs = Arrays.asList("Critical canary", "Apprehensive eagle",
        "Intimidating pigeon", "Pedantic gull", "Frisky finch");
    runWrite(inputs, /* windowed */ true);
  }

  /**
   * Performs a Write transform and verifies the Write transform calls the appropriate methods on
   * a test sink in the correct order, as well as verifies that the elements of a PCollection are
   * written to the sink.
   */
  public void runWrite(List<String> inputs, boolean windowed) {
    // Flag to validate that the pipeline options are passed to the Sink
    String[] args = {"--testFlag=test_value"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(WriteOptions.class);
    Pipeline p = TestPipeline.create(options);

    // Clear the sink's contents.
    sinkContents.clear();

    // Construct the input PCollection and test Sink.
    PCollection<String> input;
    if (windowed) {
      List<Long> timestamps = new ArrayList<>();
      for (long i = 0; i < inputs.size(); i++) {
        timestamps.add(i + 1);
      }
      input = p.apply(Create.timestamped(inputs, timestamps).withCoder(StringUtf8Coder.of()))
               .apply(Window.<String>into(FixedWindows.of(new Duration(2))));
    } else {
      input = p.apply(Create.of(inputs).withCoder(StringUtf8Coder.of()));
    }
    TestSink sink = new TestSink();

    input.apply(Write.to(sink));

    p.run();
    assertThat(sinkContents, containsInAnyOrder(inputs.toArray()));
    assertTrue(sink.hasCorrectState());
  }

  // Test sink and associated write operation and writer. TestSink, TestWriteOperation, and
  // TestWriter each verify that the sequence of method calls is consistent with the specification
  // of the Write PTransform.
  private static class TestSink extends Sink<String> {
    private static final long serialVersionUID = 0;
    private boolean createCalled = false;
    private boolean validateCalled = false;

    @Override
    public WriteOperation<String, ?> createWriteOperation(PipelineOptions options) {
      assertTrue(validateCalled);
      assertTestFlagPresent(options);
      createCalled = true;
      return new TestSinkWriteOperation(this);
    }

    @Override
    public void validate(PipelineOptions options) {
      assertTestFlagPresent(options);
      validateCalled = true;
    }

    private void assertTestFlagPresent(PipelineOptions options) {
      assertEquals("test_value", options.as(WriteOptions.class).getTestFlag());
    }

    private boolean hasCorrectState() {
      return validateCalled && createCalled;
    }
  }

  private static class TestSinkWriteOperation extends WriteOperation<String, TestWriterResult> {
    private static final long serialVersionUID = 0;

    private enum State {
      INITIAL,
      INITIALIZED,
      FINALIZED
    }

    private State state = State.INITIAL;
    private boolean coderCalled = false;

    private final TestSink sink;

    public TestSinkWriteOperation(TestSink sink) {
      this.sink = sink;
    }

    @Override
    public TestSink getSink() {
      return sink;
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {
      assertEquals("test_value", options.as(WriteOptions.class).getTestFlag());
      assertThat(state, anyOf(equalTo(State.INITIAL), equalTo(State.INITIALIZED)));
      state = State.INITIALIZED;
    }

    @Override
    public void finalize(Iterable<TestWriterResult> bundleResults, PipelineOptions options)
        throws Exception {
      assertEquals("test_value", options.as(WriteOptions.class).getTestFlag());
      assertEquals(State.INITIALIZED, state);
      // The coder for the test writer results should've been called.
      assertTrue(coderCalled);
      Set<String> idSet = new HashSet<>();
      int resultCount = 0;
      state = State.FINALIZED;
      for (TestWriterResult result : bundleResults) {
        resultCount += 1;
        idSet.add(result.uId);
        // Add the elements that were written to the sink's contents.
        sinkContents.addAll(result.elementsWritten);
      }
      // Each result came from a unique id.
      assertEquals(resultCount, idSet.size());
    }

    @Override
    public Writer<String, TestWriterResult> createWriter(PipelineOptions options) {
      return new TestSinkWriter(this);
    }

    @Override
    public Coder<TestWriterResult> getWriterResultCoder() {
      coderCalled = true;
      return SerializableCoder.of(TestWriterResult.class);
    }
  }

  private static class TestWriterResult implements Serializable {
    private static final long serialVersionUID = 0;
    String uId;
    List<String> elementsWritten;

    public TestWriterResult(String uId, List<String> elementsWritten) {
      this.uId = uId;
      this.elementsWritten = elementsWritten;
    }
  }

  private static class TestSinkWriter extends Writer<String, TestWriterResult> {
    private enum State {
      INITIAL,
      OPENED,
      WRITING,
      CLOSED
    }

    private State state = State.INITIAL;
    private List<String> elementsWritten = new ArrayList<>();
    private String uId;

    private final TestSinkWriteOperation writeOperation;

    public TestSinkWriter(TestSinkWriteOperation writeOperation) {
      this.writeOperation = writeOperation;
    }

    @Override
    public TestSinkWriteOperation getWriteOperation() {
      return writeOperation;
    }

    @Override
    public void open(String uId) throws Exception {
      this.uId = uId;
      assertEquals(State.INITIAL, state);
      state = State.OPENED;
    }

    @Override
    public void write(String value) throws Exception {
      assertThat(state, anyOf(equalTo(State.OPENED), equalTo(State.WRITING)));
      state = State.WRITING;
      elementsWritten.add(value);
    }

    @Override
    public TestWriterResult close() throws Exception {
      assertThat(state, anyOf(equalTo(State.OPENED), equalTo(State.WRITING)));
      state = State.CLOSED;
      return new TestWriterResult(uId, elementsWritten);
    }
  }

  /**
   * Options for test, exposed for PipelineOptionsFactory.
   */
  public static interface WriteOptions extends TestPipelineOptions {
    @Description("Test flag and value")
    String getTestFlag();

    void setTestFlag(String value);
  }
}
