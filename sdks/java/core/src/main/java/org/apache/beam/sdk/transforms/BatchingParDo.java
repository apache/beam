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
import org.apache.beam.sdk.values.PCollection;

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
public class BatchingParDo<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private final long batchSize;
  private final SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn;

  private BatchingParDo(
      long batchSize,
      SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn) {
    this.batchSize = batchSize;
    this.perBatchFn = perBatchFn;
  }

  public static <InputT, OutputT> BatchingParDo<InputT, OutputT> via(
      long batchSize,
      SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn) {
    return new BatchingParDo<>(batchSize, perBatchFn);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> input) {
    PCollection<OutputT> output = input.apply(ParDo.of(new BatchingDoFn<>(batchSize, perBatchFn)));
    return output;
  }

  @Override
  protected Coder<InputT> getDefaultOutputCoder(PCollection<InputT> input) {
    return input.getCoder();
  }

  @VisibleForTesting
  static class BatchingDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private final long batchSize;
    private final SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn;
    private ArrayList<InputT> batch;

    BatchingDoFn(
        long batchSize,
        SimpleFunction<? super Iterable<InputT>, ? extends Iterable<OutputT>> perBatchFn) {
      this.batch = new ArrayList<>();
      this.batchSize = batchSize;
      this.perBatchFn = perBatchFn;
    }

    @ProcessElement
    //TODO use state API to process elements trans-bundles
    // TODO make that elements from different windows are ouput in different window batches. Use timer to detect the end of the window
    public void processElement(ProcessContext c) {
      batch.add(c.element());
      if (batch.size() >= batchSize) {
        Iterable<OutputT> batchOutput = perBatchFn.apply(batch);
        for (OutputT element : batchOutput) {
          c.output(element);
        }
        batch.clear();
      }
    }

    @FinishBundle
    public void finishBundle(Context context) throws Exception {
      if (batch.size() > 0) {
        Iterable<OutputT> batchOutput = perBatchFn.apply(batch);
        for (OutputT element : batchOutput) {
          context.output(element);
        }
        batch.clear();
      }
    }
  }
}
