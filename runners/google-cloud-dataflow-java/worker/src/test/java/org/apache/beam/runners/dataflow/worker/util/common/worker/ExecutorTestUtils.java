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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Observable;
import java.util.Observer;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Utilities for tests. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
})
public class ExecutorTestUtils {
  // Do not instantiate.
  private ExecutorTestUtils() {}

  /** An Operation with a specified number of outputs. */
  public static class TestOperation extends Operation {
    public TestOperation(int numOutputs, OperationContext contexts) {
      this(numOutputs, new CounterSet(), contexts);
    }

    TestOperation(int numOutputs, CounterSet counterSet, OperationContext contexts) {
      super(createOutputReceivers(numOutputs, counterSet), contexts);
    }

    private static OutputReceiver[] createOutputReceivers(int numOutputs, CounterSet counterSet) {
      OutputReceiver[] receivers = new OutputReceiver[numOutputs];
      for (int i = 0; i < numOutputs; i++) {
        receivers[i] =
            new TestOutputReceiver(
                "out_" + i, new ElementByteSizeObservableCoder(StringUtf8Coder.of()), counterSet);
      }
      return receivers;
    }
  }

  /** A {@code Reader<String>} that yields a specified set of values. */
  public static class TestReader extends NativeReader<String> {
    private final List<String> inputs;
    boolean closed = false;
    boolean aborted = false;

    public TestReader(String... inputs) {
      this.inputs = Arrays.asList(inputs);
    }

    @Override
    public NativeReaderIterator<String> iterator() {
      return new TestReaderIterator(inputs);
    }

    class TestReaderIterator extends NativeReaderIterator<String> {
      private final List<String> inputs;
      private int currentIndex;

      public TestReaderIterator(List<String> inputs) {
        this.inputs = ImmutableList.copyOf(inputs);
      }

      @Override
      public boolean start() {
        if (inputs.isEmpty()) {
          return false;
        }
        notifyElementRead(getCurrent().length());
        return true;
      }

      @Override
      public boolean advance() throws IOException {
        if (currentIndex >= inputs.size() - 1) {
          return false;
        }
        ++currentIndex;
        notifyElementRead(getCurrent().length());
        return true;
      }

      @Override
      public String getCurrent() throws NoSuchElementException {
        return inputs.get(currentIndex);
      }

      @Override
      public void close() {
        assertFalse(closed);
        closed = true;
      }

      @Override
      public void abort() {
        assertFalse(aborted);
        aborted = true;
      }
    }
  }

  /**
   * An Observer that stores all sizes into an ArrayList, to compare against the gold standard
   * during testing.
   */
  public static class TestReaderObserver implements Observer {
    private final List<Integer> sizes;

    public TestReaderObserver(NativeReader reader) {
      this(reader, new ArrayList<>());
    }

    public TestReaderObserver(NativeReader reader, List<Integer> sizes) {
      this.sizes = sizes;
      reader.addObserver(this);
    }

    @Override
    public void update(Observable obs, Object obj) {
      sizes.add((int) (long) obj);
    }

    public List<Integer> getActualSizes() {
      return sizes;
    }
  }

  /** A {@code Sink<String>} that allows the output elements to be retrieved. */
  public static class TestSink extends Sink<String> {
    List<String> outputElems = new ArrayList<>();
    boolean closed = false;
    boolean aborted = false;

    @Override
    public SinkWriter<String> writer() {
      return new TestSinkWriter();
    }

    class TestSinkWriter implements SinkWriter<String> {
      @Override
      public long add(String outputElem) {
        outputElems.add(outputElem);
        return outputElem.length();
      }

      @Override
      public void close() {
        assertFalse(closed);
        closed = true;
      }

      @Override
      public void abort() throws IOException {
        close();
      }
    }
  }
}
