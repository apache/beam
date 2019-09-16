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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/**
 * A {@link ParDoFn} over per-key iterables that applies an underlying {@link ParDoFn} to the
 * elements of an underlying iterable, then processes its timers.
 *
 * <p>Each input element must be a {@link KV} where the value is an iterable of {@link WindowedValue
 * WindowedValues}.
 */
class BatchModeUngroupingParDoFn<K, V> implements ParDoFn {

  private final ParDoFn underlyingParDoFn;
  private final BatchModeExecutionContext.StepContext stepContext;

  BatchModeUngroupingParDoFn(
      BatchModeExecutionContext.StepContext stepContext, ParDoFn underlyingParDoFn) {
    this.underlyingParDoFn = underlyingParDoFn;
    this.stepContext = stepContext;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    underlyingParDoFn.startBundle(receivers);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object untypedElem) throws Exception {
    WindowedValue<?> windowedValue = (WindowedValue<?>) untypedElem;

    KV<K, Iterable<KV<Instant, WindowedValue<V>>>> gbkElem =
        (KV<K, Iterable<KV<Instant, WindowedValue<V>>>>) windowedValue.getValue();

    // Each GBK output is the beginning of a key
    stepContext.setKey(gbkElem.getKey());

    for (KV<Instant, WindowedValue<V>> timestampedElem : gbkElem.getValue()) {
      underlyingParDoFn.processElement(timestampedElem.getValue());
    }

    // Process all the timers for the key, since the watermark is moved to infinity
    underlyingParDoFn.processTimers();
  }

  @Override
  public void processTimers() throws Exception {
    // The timers for the underlying ParDoFn are processed at the end of each element
  }

  @Override
  public void finishBundle() throws Exception {
    underlyingParDoFn.finishBundle();
  }

  @Override
  public void abort() throws Exception {
    underlyingParDoFn.abort();
  }
}
