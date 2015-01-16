/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

/**
 * A write operation.
 */
public class WriteOperation extends ReceivingOperation {
  /**
   * The Sink this operation writes to.
   */
  public final Sink<?> sink;

  /**
   * The total byte counter for all data written by this operation.
   */
  final Counter<Long> byteCount;

  /**
   * The Sink's writer this operation writes to, created by start().
   */
  Sink.SinkWriter<Object> writer;

  public WriteOperation(String operationName,
                        Sink<?> sink,
                        OutputReceiver[] receivers,
                        String counterPrefix,
                        CounterSet.AddCounterMutator addCounterMutator,
                        StateSampler stateSampler) {
    super(operationName, receivers,
          counterPrefix, addCounterMutator, stateSampler);
    this.sink = sink;
    this.byteCount = addCounterMutator.addCounter(
        Counter.longs(bytesCounterName(counterPrefix, operationName), SUM));
  }

  /** Invoked by tests. */
  public WriteOperation(Sink<?> sink,
                        String counterPrefix,
                        CounterSet.AddCounterMutator addCounterMutator,
                        StateSampler stateSampler) {
    this("WriteOperation", sink, new OutputReceiver[]{ },
         counterPrefix, addCounterMutator, stateSampler);
  }

  protected String bytesCounterName(String counterPrefix,
                                    String operationName) {
    return operationName + "-ByteCount";
  }

  public Sink<?> getSink() {
    return sink;
  }

  @Override
  public void start() throws Exception {
    try (StateSampler.ScopedState start =
        stateSampler.scopedState(startState)) {
      assert start != null;
      super.start();
      writer = (Sink.SinkWriter<Object>) sink.writer();
    }
  }

  @Override
  public void process(Object outputElem) throws Exception {
    try (StateSampler.ScopedState process =
        stateSampler.scopedState(processState)) {
      assert process != null;
      checkStarted();
      byteCount.addValue(writer.add(outputElem));
    }
  }

  @Override
  public void finish() throws Exception {
    try (StateSampler.ScopedState finish =
        stateSampler.scopedState(finishState)) {
      assert finish != null;
      checkStarted();
      writer.close();
      super.finish();
    }
  }

  @Override
  public boolean supportsRestart() {
    return sink.supportsRestart();
  }

  public Counter<Long> getByteCount() {
    return byteCount;
  }
}
