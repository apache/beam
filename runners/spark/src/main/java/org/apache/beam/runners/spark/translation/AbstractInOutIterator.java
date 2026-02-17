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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.translation.streaming.ParDoStateUpdateFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import scala.Tuple2;

/**
 * Abstract base class for iterators that process Spark input data and produce corresponding output
 * values. This class serves as a common base for both bounded and unbounded processing strategies
 * in the Spark runner.
 *
 * <p>The class extends Guava's {@link AbstractIterator} and provides common functionality for
 * iterating through input elements, processing them using a DoFnRunner, and producing output
 * elements as tuples of {@link TupleTag} and {@link WindowedValue} pairs.
 *
 * @param <K> The key type for the processing context
 * @param <InputT> The input element type to be processed
 * @param <OutputT> The output element type after processing
 */
public abstract class AbstractInOutIterator<K, InputT, OutputT>
    extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>> {
  protected final SparkProcessContext<K, InputT, OutputT> ctx;

  protected AbstractInOutIterator(SparkProcessContext<K, InputT, OutputT> ctx) {
    this.ctx = ctx;
  }

  /**
   * Fires a timer using the DoFnRunner from the context and performs cleanup afterwards.
   *
   * <p>After firing the timer, if the timer data iterator is an instance of {@link
   * ParDoStateUpdateFn.SparkTimerInternalsIterator}, the fired timer will be deleted as part of
   * cleanup to prevent re-firing of the same timer.
   *
   * @param timer The timer data containing information about the timer to fire
   * @throws IllegalArgumentException If the timer namespace is not a {@link
   *     StateNamespaces.WindowNamespace}
   */
  public void fireTimer(TimerInternals.TimerData timer) {
    StateNamespace namespace = timer.getNamespace();
    checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
    BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    try {
      this.ctx
          .getDoFnRunner()
          .onTimer(
              timer.getTimerId(),
              timer.getTimerFamilyId(),
              this.ctx.getKey(),
              window,
              timer.getTimestamp(),
              timer.getOutputTimestamp(),
              timer.getDomain());
    } finally {
      if (this.ctx.getTimerDataIterator()
          instanceof ParDoStateUpdateFn.SparkTimerInternalsIterator) {
        final ParDoStateUpdateFn.SparkTimerInternalsIterator timerDataIterator =
            (ParDoStateUpdateFn.SparkTimerInternalsIterator) this.ctx.getTimerDataIterator();
        timerDataIterator.deleteTimer(timer);
      }
    }
  }
}
