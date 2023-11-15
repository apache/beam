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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** An OutputReceiver that allows the output elements to be retrieved. */
public class TestOutputReceiver extends OutputReceiver {
  private static final String OBJECT_COUNTER_NAME = "-ObjectCount";
  private static final String MEAN_BYTE_COUNTER_NAME = "-MeanByteCount";

  @VisibleForTesting public final List<Object> outputElems = new ArrayList<>();

  public TestOutputReceiver(CounterSet counterSet) {
    this("test_receiver_out", counterSet);
  }

  public TestOutputReceiver(CounterSet counterSet, NameContext nameContext) {
    this(
        "test_receiver_out",
        new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
        counterSet,
        nameContext);
  }

  public TestOutputReceiver(Coder<?> coder, NameContext nameContext) {
    this(new ElementByteSizeObservableCoder<>(coder), new CounterSet(), nameContext);
  }

  public TestOutputReceiver(Coder<?> coder, CounterSet counterSet) {
    this("test_receiver_out", new ElementByteSizeObservableCoder<>(coder), counterSet, null);
  }

  public TestOutputReceiver(String outputName, CounterSet counterSet) {
    this(outputName, new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()), counterSet, null);
  }

  public TestOutputReceiver(
      ElementByteSizeObservable<?> elementByteSizeObservable, CounterSet counterSet) {
    this("test_receiver_out", elementByteSizeObservable, counterSet, null);
  }

  public TestOutputReceiver(
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterSet counterSet,
      NameContext nameContext) {
    this("test_receiver_out", elementByteSizeObservable, counterSet, nameContext);
  }

  public TestOutputReceiver(
      String string,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory) {
    this(string, elementByteSizeObservable, counterFactory, null);
  }

  public TestOutputReceiver(
      String string,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext) {
    ElementCounter outputCounter =
        new TestOutputCounter(string, elementByteSizeObservable, counterFactory, nameContext);
    addOutputCounter(outputCounter);
  }

  @Override
  public void process(Object elem) throws Exception {
    super.process(elem);
    outputElems.add(elem);
  }

  /** TestOutputCounter that samples every element. */
  public static class TestOutputCounter extends OutputObjectAndByteCounter {
    public TestOutputCounter(NameContext nameContext) {
      this(new CounterSet(), nameContext);
    }

    @SuppressWarnings("rawtypes")
    public TestOutputCounter(CounterSet counters) {
      this(
          "output_name",
          new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
          counters,
          null);
    }

    public TestOutputCounter(CounterSet counters, NameContext nameContext) {
      this(
          "output_name",
          new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
          counters,
          nameContext);
    }

    public TestOutputCounter(
        String outputName,
        ElementByteSizeObservable<?> elementByteSizeObservable,
        CounterFactory counterFactory,
        NameContext nameContext) {
      super(elementByteSizeObservable, counterFactory, nameContext);
      this.countObject(outputName + OBJECT_COUNTER_NAME);
      this.countMeanByte(outputName + MEAN_BYTE_COUNTER_NAME);
    }

    @Override
    protected boolean sampleElement() {
      return true;
    }

    public static CounterName getObjectCounterName(String prefix) {
      return CounterName.named(prefix + OBJECT_COUNTER_NAME);
    }

    public static CounterName getMeanByteCounterName(String prefix) {
      return CounterName.named(prefix + MEAN_BYTE_COUNTER_NAME);
    }
  }
}
