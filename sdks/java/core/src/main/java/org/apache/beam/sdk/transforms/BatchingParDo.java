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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import org.apache.beam.sdk.coders.Coder;
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
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} that allows to compute elements in batch of desired size.
 * Elements are added to a buffer. When the buffer reaches {@code batchSize},
 * it is then processed through a user {@link SimpleFunction<ArrayList<InputT>, ArrayList<OutputT>> perBatchFn} function.
 * The output elements then are added to the output {@link PCollection}. Windows are preserved (batches contain elements from the same window).
 * Batching is done trans-bundles (batches may contain elements from more than one bundle)
 * <p>Example (batch call a webservice and get return codes)</p>
 * <pre>
 * {@code SimpleFunction<ArrayList<String>, ArrayList<Integer>> perBatchFn = new SimpleFunction<>() {
 * @Override
 * public ArrayList<Integer> apply(ArrayList<String> input) {
 * ArrayList<String> results = webserviceCall(input)
 * for (String element : results) {
 *  output.add(extractReturnCode(element));
 * }
 * return output;
 * }
 * batchSize = 100;
 * ...
 * pipeline.apply(BatchingParDo.via(BATCH_SIZE, perBatchFn));
 * ...
 * pipeline.run();
 * </pre>
 **
 *
 */
public class BatchingParDo<K, InputT, OutputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<OutputT>> {

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
  public PCollection<OutputT> expand(PCollection<KV<K, InputT>> input) {
    Duration allowedLateness = input.getWindowingStrategy().getAllowedLateness();
    PCollection<OutputT> output =
        input.apply(
            ParDo.of(
                new BatchingDoFn<K, InputT, OutputT>(
                    batchSize,
                    perBatchFn,
                    allowedLateness,
                    (Coder<InputT>) input.getCoder().getCoderArguments().get(1))));
    return output;
  }

  @VisibleForTesting
  static class BatchingDoFn<K, InputT, OutputT> extends DoFn<KV<K, InputT>, OutputT> {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BatchingDoFn.class);
    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String BATCH_ID = "batch";
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";
    private final long batchSize;
    private final SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn;
    private final Duration allowedLateness;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId(BATCH_ID)
    private final StateSpec<Object, BagState<InputT>> batchSpec;

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<Object, ValueState<Long>> numElementsInBatchSpec;

    private final long prefetchFrequency;

    BatchingDoFn(
        long batchSize,
        SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn,
        Duration allowedLateness,
        Coder<InputT> inputCoder) {
      this.batchSize = batchSize;
      this.perBatchFn = perBatchFn;
      this.allowedLateness = allowedLateness;
      this.batchSpec = StateSpecs.bag(inputCoder);
      this.numElementsInBatchSpec = StateSpecs.value(VarLongCoder.of());
      // prefetch every 20% of batchSize elements. Do not prefetch if batchSize is too little
      this.prefetchFrequency = ((batchSize / 5) <= 1) ? Long.MAX_VALUE:(batchSize / 5);
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_WINDOW_ID) Timer timer,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) ValueState<Long> numElementsInBatch,
        ProcessContext c,
        BoundedWindow window) {
      Instant firingInstant = window.maxTimestamp().plus(allowedLateness);
      Duration delay = new Duration(Instant.now(), firingInstant);
      //TODO LOGGER.debug or remove
      LOGGER.info(
          String.format(
              "***** DELAY ***** of %d ms in timer set for window %s",
              delay.getMillis(), window.toString()));

      // Timers are scoped to the window. A timer can be set only for a single time per scope.
      // So it will be set only once per window????
      timer.setForNowPlus(delay);

      batch.add(c.element().getValue());
      Long num = numElementsInBatch.read();
      if (num == null){
        num = 0L;
      }
      num++;
      numElementsInBatch.write(num);
      if ((num > 0) && (num % prefetchFrequency == 0)) {
        //prefetch data and modify batch state (readLater() modifies this)
        batch.readLater();
      }
      if (num >= batchSize) {
        //TODO LOGGER.debug or remove
        LOGGER.info(
            String.format(
                "***** FLUSH ***** due to batch size in window %s",
                window.toString(), window.toString()));
        flushBatch(c, batch, numElementsInBatch);
      }
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onTimerCallback(
        OnTimerContext context,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID) ValueState<Long> numElementsInBatch) {
      //TODO LOGGER.debug or remove
      LOGGER.info(
          String.format(
              "***** TIMER FIRES ***** for window %s",
              context.window().toString(), context.window().toString()));
      flushBatch(context, batch, numElementsInBatch);
    }

    private void flushBatch(
        Context c, BagState<InputT> batch, ValueState<Long> numElementsInBatch) {
      Iterable<OutputT> batchOutput = perBatchFn.apply(batch.read());
      for (OutputT element : batchOutput) {
        c.output(element);
      }
      batch.clear();
      numElementsInBatch.write(0L);
    }
  }
}
