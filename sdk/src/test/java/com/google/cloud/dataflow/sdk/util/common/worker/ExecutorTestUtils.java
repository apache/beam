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

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.common.collect.ImmutableList;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Observable;
import java.util.Observer;

/**
 * Utilities for tests.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ExecutorTestUtils {
  // Do not instantiate.
  private ExecutorTestUtils() {}

  /** An Operation with a specified number of outputs. */
  public static class TestOperation extends Operation {
    public TestOperation(int numOutputs) {
      this(numOutputs, new CounterSet());
    }

    TestOperation(int numOutputs, CounterSet counters) {
      this(numOutputs, counters, "test-");
    }

    TestOperation(int numOutputs, CounterSet counters, String counterPrefix) {
      this(numOutputs, counterPrefix, counters.getAddCounterMutator(),
          new StateSampler(counterPrefix, counters.getAddCounterMutator()));
    }

    TestOperation(int numOutputs, String counterPrefix,
        CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) {
      super("TestOperation",
          createOutputReceivers(numOutputs, counterPrefix, addCounterMutator),
          counterPrefix, addCounterMutator, stateSampler);
    }

    private static OutputReceiver[] createOutputReceivers(int numOutputs, String counterPrefix,
        CounterSet.AddCounterMutator addCounterMutator) {
      OutputReceiver[] receivers = new OutputReceiver[numOutputs];
      for (int i = 0; i < numOutputs; i++) {
        receivers[i] = new TestOutputReceiver("out_" + i,
            new ElementByteSizeObservableCoder(StringUtf8Coder.of()), addCounterMutator);
      }
      return receivers;
    }
  }


  /** A {@code Reader<String>} that yields a specified set of values. */
  public static class TestReader extends NativeReader<String> {
    private final List<String> inputs;

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
      private boolean closed = false;

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
        Assert.assertFalse(closed);
        closed = true;
      }
    }
  }

  /**
   * An Observer that stores all sizes into an ArrayList, to compare
   * against the gold standard during testing.
   */
  public static class TestReaderObserver implements Observer {
    private final List<Integer> sizes;

    public TestReaderObserver(NativeReader reader) {
      this(reader, new ArrayList<Integer>());
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
        Assert.assertFalse(closed);
        closed = true;
      }
    }
  }
}
