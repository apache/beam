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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerSpec;
import org.apache.beam.sdk.util.TimerSpecs;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} that allows to compute elements in batch of desired size. The input {@link
 * PCollection} needs to be a {@code PCollection<KV>}. Elements that must belong to the same batch
 * need to have the same key. Elements are added to a buffer. When the buffer reaches {@code
 * batchSize}, it is then processed through a user {@link SimpleFunction perBatchFn} function. The
 * output elements then are added to the output {@link PCollection}. Windows are preserved (batches
 * contain elements from the same window). Batching is done trans-bundles (batches may contain
 * elements from more than one bundle)
 *
 * <p>Example (batch call a webservice and get return codes)
 *
 * <pre>{@code
 * SimpleFunction<Iterable<String>, Iterable<String>> perBatchFn =
 * new SimpleFunction<Iterable<String>, Iterable<String>>() {
 *   {@literal @}Override
 *   public Iterable<String> apply(Iterable<String> input) {
 *     ArrayList<String> results = callWebService(input);
 *     return results;
 *     }
 *  };
 *  ...
 *  Pipeline pipeline = Pipeline.create(...);
 *  ...
 *  long batchSize = 100L;
 *  pipeline.apply(BatchingParDo.via(batchSize, perBatchFn))
 *          .setCoder(StringUtf8Coder.of());
 *  pipeline.run();
 * }</pre>
 * *
 */
public class BatchingParDo<K, InputT, OutputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

  private final long batchSize;
  private final SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn;

  private BatchingParDo(
      long batchSize,
      SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn) {
    this.batchSize = batchSize;
    this.perBatchFn = perBatchFn;
  }

  public static <K, InputT, OutputT> BatchingParDo<K, InputT, OutputT> via(
      long batchSize,
      SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn) {
    return new BatchingParDo<>(batchSize, perBatchFn);
  }

  @Override
  public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
    Duration allowedLateness = input.getWindowingStrategy().getAllowedLateness();

    checkArgument(input.getCoder() instanceof KvCoder,
        "coder specified in the input PCollection is not a KvCoder");
    KvCoder inputCoder = (KvCoder)input.getCoder();
    Coder<K> keyCoder = (Coder<K>)inputCoder.getCoderArguments().get(0);
    Coder<InputT> valueCoder = (Coder<InputT>)inputCoder.getCoderArguments().get(1);


    PCollection<KV<K, OutputT>> output =
        input.apply(
            ParDo.of(
                new BatchingDoFn<>(
                    batchSize,
                    perBatchFn,
                    allowedLateness,
                    keyCoder,
                    valueCoder)));
    return output;
  }

  @VisibleForTesting
  static class BatchingDoFn<K, InputT, OutputT> extends DoFn<KV<K, InputT>, KV<K, OutputT>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchingDoFn.class);
    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String BATCH_ID = "batch";
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";
    private static final String TIMER_ALREADY_SET_ID = "timerAlreadySet";
    private static final String KEY_ID = "key";
    private final long batchSize;
    private final SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn;
    private final Duration allowedLateness;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId(TIMER_ALREADY_SET_ID)
    private final StateSpec<Object, ValueState<Integer>> timerAlreadySetForWindow;

    @StateId(BATCH_ID)
    private final StateSpec<Object, BagState<InputT>> batchSpec;

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<Object, ValueState<Long>> numElementsInBatchSpec;

    @StateId(KEY_ID)
    private final StateSpec<Object, ValueState<K>> keySpec;

    private final long prefetchFrequency;

    BatchingDoFn(
        long batchSize,
        SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn,
        Duration allowedLateness,
        Coder<K> inputKeyCoder,
        Coder<InputT> inputValueCoder) {
      this.batchSize = batchSize;
      this.perBatchFn = perBatchFn;
      this.allowedLateness = allowedLateness;
      this.batchSpec = StateSpecs.bag(inputValueCoder);
      this.numElementsInBatchSpec = StateSpecs.value(VarLongCoder.of());
      this.timerAlreadySetForWindow = StateSpecs.value(VarIntCoder.of());
      this.keySpec = StateSpecs.value(inputKeyCoder);
      // prefetch every 20% of batchSize elements. Do not prefetch if batchSize is too little
      this.prefetchFrequency = ((batchSize / 5) <= 1) ? Long.MAX_VALUE : (batchSize / 5);
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_WINDOW_ID) Timer timer,
        @StateId(TIMER_ALREADY_SET_ID) ValueState<Integer> timerAlreadySetForWindow,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) ValueState<Long> numElementsInBatch,
        @StateId(KEY_ID) ValueState<K> key,
        ProcessContext c,
        BoundedWindow window) {
      Instant firingInstant = window.maxTimestamp().plus(allowedLateness);
      // Timers are scoped to the window. A timer can be set only for a single time per scope.
      // But prevent to set it at each element (set it once per window)
      Integer isSet = timerAlreadySetForWindow.read();
      if (isSet == null) {
        LOGGER.debug(
            "*** SET TIMER *** to point in time %s for window %s",
            firingInstant.toString(), window.toString());
        timer.set(firingInstant);
        timerAlreadySetForWindow.write(1);
      }
      key.write(c.element().getKey());
      batch.add(c.element().getValue());
      LOGGER.debug("*** BATCH *** Add element for window %s ", window.toString());
      Long num = numElementsInBatch.read();
      if (num == null) {
        num = 0L;
      }
      num++;
      numElementsInBatch.write(num);
      if ((num > 0) && (num % prefetchFrequency == 0)) {
        //prefetch data and modify batch state (readLater() modifies this)
        batch.readLater();
      }
      if (num >= batchSize) {
        LOGGER.debug("*** END OF BATCH *** for window %s", window.toString());
        flushBatch(c, key, batch, numElementsInBatch);
      }
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onTimerCallback(
        OnTimerContext context,
        @StateId(KEY_ID) ValueState<K> key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) ValueState<Long> numElementsInBatch,
        BoundedWindow window) {
      LOGGER.debug(
          "*** END OF WINDOW *** for timer timestamp %s in windows %s",
          context.timestamp(), window.toString());
      flushBatch(context, key, batch, numElementsInBatch);
    }

    private void flushBatch(
        Context c, ValueState<K> key, BagState<InputT> batch, ValueState<Long> numElementsInBatch) {
      Iterable<OutputT> batchOutput = perBatchFn.apply(batch.read());
      for (OutputT element : batchOutput) {
        c.output(KV.of(key.read(), element));
      }
      batch.clear();
      LOGGER.debug("*** BATCH *** clear");
      numElementsInBatch.write(0L);
    }
  }
}
