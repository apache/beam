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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

/**
 * An OutputReceiver that allows the output elements to be retrieved.
 */
public class TestOutputReceiver extends OutputReceiver {
  private static final String OBJECT_COUNTER_NAME = "-ObjectCount";
  private static final String MEAN_BYTE_COUNTER_NAME = "-MeanByteCount";

  @VisibleForTesting
  final List<Object> outputElems = new ArrayList<>();

  public TestOutputReceiver(CounterSet counterSet) {
    this("test_receiver_out", counterSet);
  }

  public TestOutputReceiver(Coder<?> coder) {
    this(coder, new CounterSet());
  }

  public TestOutputReceiver(Coder<?> coder, CounterSet counterSet) {
    this("test_receiver_out", new ElementByteSizeObservableCoder<>(coder), counterSet);
  }

  public TestOutputReceiver(String outputName, CounterSet counterSet) {
    this(outputName, new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()), counterSet);
  }

  public TestOutputReceiver(
      ElementByteSizeObservable<?> elementByteSizeObservable, CounterSet counterSet) {
    this("test_receiver_out", elementByteSizeObservable, counterSet);
  }

  public TestOutputReceiver(String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterSet counterSet) {
    this(outputName, elementByteSizeObservable, counterSet.getAddCounterMutator());
  }

  public TestOutputReceiver(String string, ElementByteSizeObservable<?> elementByteSizeObservable,
      AddCounterMutator addCounterMutator) {
    ElementCounter outputCounter =
        new TestOutputCounter(string, elementByteSizeObservable, addCounterMutator);
    addOutputCounter(outputCounter);
  }

  @Override
  public void process(Object elem) throws Exception {
    super.process(elem);
    outputElems.add(elem);
  }

  /**
   * TestOutputCounter that samples every element.
   */
  public static class TestOutputCounter extends OutputObjectAndByteCounter {
    public TestOutputCounter() {
      this(new CounterSet());
    }

    @SuppressWarnings("rawtypes")
    public TestOutputCounter(CounterSet counters) {
      this("output_name", new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
          counters.getAddCounterMutator());
    }

    public TestOutputCounter(String outputName,
        ElementByteSizeObservable<?> elementByteSizeObservable,
        CounterSet.AddCounterMutator addCounterMutator) {
      super(elementByteSizeObservable, addCounterMutator);
      this.countObject(outputName + OBJECT_COUNTER_NAME);
      this.countMeanByte(outputName + MEAN_BYTE_COUNTER_NAME);
    }

    @Override
    protected boolean sampleElement() {
      return true;
    }

    @VisibleForTesting
    static String getObjectCounterName(String prefix) {
      return prefix + OBJECT_COUNTER_NAME;
    }

    @VisibleForTesting
    static String getMeanByteCounterName(String prefix) {
      return prefix + MEAN_BYTE_COUNTER_NAME;
    }
  }
}
