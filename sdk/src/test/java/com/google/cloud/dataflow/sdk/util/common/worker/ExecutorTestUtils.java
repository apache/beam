/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
          createOutputReceivers(numOutputs, counterPrefix, addCounterMutator, stateSampler),
          counterPrefix, addCounterMutator, stateSampler);
    }

    private static OutputReceiver[] createOutputReceivers(int numOutputs, String counterPrefix,
        CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) {
      OutputReceiver[] receivers = new OutputReceiver[numOutputs];
      for (int i = 0; i < numOutputs; i++) {
        receivers[i] =
            new OutputReceiver("out_" + i, new ElementByteSizeObservableCoder(StringUtf8Coder.of()),
                counterPrefix, addCounterMutator);
      }
      return receivers;
    }
  }

  /** An OutputReceiver that allows the output elements to be retrieved. */
  public static class TestReceiver extends OutputReceiver {
    List<Object> outputElems = new ArrayList<>();

    public TestReceiver(CounterSet counterSet) {
      this("test_receiver_out", counterSet);
    }

    public TestReceiver(Coder<?> coder) {
      this(coder, new CounterSet());
    }

    public TestReceiver(Coder<?> coder, CounterSet counterSet) {
      this("test_receiver_out", new ElementByteSizeObservableCoder(coder), counterSet, "test-");
    }

    public TestReceiver(CounterSet counterSet, String counterPrefix) {
      this("test_receiver_out", counterSet, counterPrefix);
    }

    public TestReceiver(String outputName, CounterSet counterSet) {
      this(outputName, counterSet, "test-");
    }

    public TestReceiver(String outputName, CounterSet counterSet, String counterPrefix) {
      this(outputName, new ElementByteSizeObservableCoder(StringUtf8Coder.of()), counterSet,
          counterPrefix);
    }

    public TestReceiver(ElementByteSizeObservable elementByteSizeObservable, CounterSet counterSet,
        String counterPrefix) {
      this("test_receiver_out", elementByteSizeObservable, counterSet, counterPrefix);
    }

    public TestReceiver(String outputName, ElementByteSizeObservable elementByteSizeObservable,
        CounterSet counterSet, String counterPrefix) {
      super(
          outputName, elementByteSizeObservable, counterPrefix, counterSet.getAddCounterMutator());
    }

    @Override
    public void process(Object elem) throws Exception {
      super.process(elem);
      outputElems.add(elem);
    }

    @Override
    protected boolean sampleElement() {
      return true;
    }
  }

  /** A {@code Reader<String>} that yields a specified set of values. */
  public static class TestReader extends Reader<String> {
    List<String> inputs = new ArrayList<>();

    public void addInput(String... inputs) {
      this.inputs.addAll(Arrays.asList(inputs));
    }

    @Override
    public ReaderIterator<String> iterator() {
      return new TestReaderIterator(inputs);
    }

    class TestReaderIterator extends AbstractReaderIterator<String> {
      Iterator<String> iter;
      boolean closed = false;

      public TestReaderIterator(List<String> inputs) {
        iter = inputs.iterator();
      }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public String next() {
        String next = iter.next();
        notifyElementRead(next.length());
        return next;
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
    private final Reader reader;
    private final List<Integer> sizes;

    public TestReaderObserver(Reader reader) {
      this(reader, new ArrayList<Integer>());
    }

    public TestReaderObserver(Reader reader, List<Integer> sizes) {
      this.reader = reader;
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
