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

package org.apache.beam.runners.samza.runtime;

import java.io.Serializable;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.task.TaskContext;
import org.joda.time.Instant;

/**
 * Interface of Samza operator for BEAM. This interface demultiplexes messages from BEAM
 * so that elements and side inputs can be handled separately in Samza. Watermark propagation
 * can be overridden so we can hold watermarks for side inputs. The output values and watermark
 * will be collected via {@link OpEmitter}.
 */
public interface Op<InT, OutT, K> extends Serializable {
  /**
   * A hook that allows initialization for any non-serializable operator state, such as getting
   * stores.
   *
   * <p>While an emitter is supplied to this function it is not usable except in the methods
   * {@link #processElement(WindowedValue, OpEmitter)},
   * {@link #processWatermark(Instant, OpEmitter)}, and
   * {@link #processSideInput(String, WindowedValue, OpEmitter)}.
   */
  default void open(Config config,
                    TaskContext taskContext,
                    TimerRegistry<KeyedTimerData<K>> timerRegistry,
                    OpEmitter<OutT> emitter) {}

  void processElement(WindowedValue<InT> inputElement, OpEmitter<OutT> emitter);

  default void processWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    emitter.emitWatermark(watermark);
  }

  default void processSideInput(String id,
                                WindowedValue<? extends Iterable<?>> elements,
                                OpEmitter<OutT> emitter) {
    throw new UnsupportedOperationException("Side inputs not supported for: " + this.getClass());
  }

  default void processSideInputWatermark(Instant watermark, OpEmitter<OutT> emitter) {}

  default void processTimer(KeyedTimerData<K> keyedTimerData) {};

  default void close() {}
}
