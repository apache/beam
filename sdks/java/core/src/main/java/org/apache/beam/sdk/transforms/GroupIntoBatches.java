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
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerSpec;
import org.apache.beam.sdk.util.TimerSpecs;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CombiningState;
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
 * A {@link PTransform} that batches inputs to a desired batch size. Batches will contain only
 * elements of a single key.
 *
 * <p>Elements are buffered until there are {@code batchSize} elements
 * buffered, at which point they are output to the output {@link PCollection}.
 *
 * <p>Windows are preserved (batches contain elements from the same window).
 * Batches may contain elements from more than one bundle
 *
 * <p>Example (batch call a webservice and get return codes)
 *
 * <pre>{@code
 *  Pipeline pipeline = Pipeline.create(...);
 *  ... // KV collection
 *  long batchSize = 100L;
 *  pipeline.apply(GroupIntoBatches.<String, String>ofSize(batchSize))
 * .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())))
 * .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, String>>() {
 * {@literal @}ProcessElement
 * public void processElement(ProcessContext c){
 * c.output(KV.of(c.element().getKey(), callWebService(c.element().getValue())));
 * }
 * }));
 *  pipeline.run();
 * }</pre>
 */
public class GroupIntoBatches<K, InputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> {

  private final long batchSize;

  private GroupIntoBatches(long batchSize) {
    this.batchSize = batchSize;
  }

  public static <K, InputT> GroupIntoBatches<K, InputT> ofSize(long batchSize) {
    return new GroupIntoBatches<>(batchSize);
  }

  @Override
  public PCollection<KV<K, Iterable<InputT>>> expand(PCollection<KV<K, InputT>> input) {
    Duration allowedLateness = input.getWindowingStrategy().getAllowedLateness();

    checkArgument(
        input.getCoder() instanceof KvCoder,
        "coder specified in the input PCollection is not a KvCoder");
    KvCoder inputCoder = (KvCoder) input.getCoder();
    Coder<K> keyCoder = (Coder<K>) inputCoder.getCoderArguments().get(0);
    Coder<InputT> valueCoder = (Coder<InputT>) inputCoder.getCoderArguments().get(1);

    return input.apply(
        ParDo.of(new GroupIntoBatchesDoFn<>(batchSize, allowedLateness, keyCoder, valueCoder)));
  }

  @VisibleForTesting
  static class GroupIntoBatchesDoFn<K, InputT>
      extends DoFn<KV<K, InputT>, KV<K, Iterable<InputT>>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupIntoBatchesDoFn.class);
    private static final String END_OF_WINDOW_ID = "endOFWindow";
    private static final String BATCH_ID = "batch";
    private static final String NUM_ELEMENTS_IN_BATCH_ID = "numElementsInBatch";
    private static final String KEY_ID = "key";
    private final long batchSize;
    private final Duration allowedLateness;

    @TimerId(END_OF_WINDOW_ID)
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId(BATCH_ID)
    private final StateSpec<BagState<InputT>> batchSpec;

    @StateId(NUM_ELEMENTS_IN_BATCH_ID)
    private final StateSpec<CombiningState<Long, Long, Long>>
        numElementsInBatchSpec;

    @StateId(KEY_ID)
    private final StateSpec<ValueState<K>> keySpec;

    private final long prefetchFrequency;

    GroupIntoBatchesDoFn(
        long batchSize,
        Duration allowedLateness,
        Coder<K> inputKeyCoder,
        Coder<InputT> inputValueCoder) {
      this.batchSize = batchSize;
      this.allowedLateness = allowedLateness;
      this.batchSpec = StateSpecs.bag(inputValueCoder);
      this.numElementsInBatchSpec =
          StateSpecs.combining(
              VarLongCoder.of(),
              new Combine.CombineFn<Long, Long, Long>() {

                @Override
                public Long createAccumulator() {
                  return 0L;
                }

                @Override
                public Long addInput(Long accumulator, Long input) {
                  return accumulator + input;
                }

                @Override
                public Long mergeAccumulators(Iterable<Long> accumulators) {
                  long sum = 0L;
                  for (Long accumulator : accumulators) {
                    sum += accumulator;
                  }
                  return sum;
                }

                @Override
                public Long extractOutput(Long accumulator) {
                  return accumulator;
                }
              });

      this.keySpec = StateSpecs.value(inputKeyCoder);
      // prefetch every 20% of batchSize elements. Do not prefetch if batchSize is too little
      this.prefetchFrequency = ((batchSize / 5) <= 1) ? Long.MAX_VALUE : (batchSize / 5);
    }

    @ProcessElement
    public void processElement(
        @TimerId(END_OF_WINDOW_ID) Timer timer,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID)
            CombiningState<Long, Long, Long> numElementsInBatch,
        @StateId(KEY_ID) ValueState<K> key,
        ProcessContext c,
        BoundedWindow window) {
      Instant windowExpires = window.maxTimestamp().plus(allowedLateness);

      LOGGER.debug(
          "*** SET TIMER *** to point in time {} for window {}",
          windowExpires.toString(), window.toString());
      timer.set(windowExpires);
      key.write(c.element().getKey());
      batch.add(c.element().getValue());
      LOGGER.debug("*** BATCH *** Add element for window {} ", window.toString());
      // blind add is supported with combiningState
      numElementsInBatch.add(1L);
      Long num = numElementsInBatch.read();
      if (num % prefetchFrequency == 0) {
        //prefetch data and modify batch state (readLater() modifies this)
        batch.readLater();
      }
      if (num >= batchSize) {
        LOGGER.debug("*** END OF BATCH *** for window {}", window.toString());
        flushBatch(c, key, batch, numElementsInBatch);
      }
    }

    @OnTimer(END_OF_WINDOW_ID)
    public void onTimerCallback(
        OnTimerContext context,
        @StateId(KEY_ID) ValueState<K> key,
        @StateId(BATCH_ID) BagState<InputT> batch,
        @StateId(NUM_ELEMENTS_IN_BATCH_ID)
            CombiningState<Long, Long, Long> numElementsInBatch,
        BoundedWindow window) {
      LOGGER.debug(
          "*** END OF WINDOW *** for timer timestamp {} in windows {}",
          context.timestamp(), window.toString());
      flushBatch(context, key, batch, numElementsInBatch);
    }

    private void flushBatch(
        Context c,
        ValueState<K> key,
        BagState<InputT> batch,
        CombiningState<Long, Long, Long> numElementsInBatch) {
      Iterable<InputT> values = batch.read();
      // when the timer fires, batch state might be empty
      if (!Iterables.isEmpty(values)) {
        c.output(KV.of(key.read(), values));
      }
      batch.clear();
      LOGGER.debug("*** BATCH *** clear");
      numElementsInBatch.clear();
    }
  }
}
