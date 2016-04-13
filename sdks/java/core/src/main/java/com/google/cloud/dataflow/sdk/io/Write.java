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
package com.google.cloud.dataflow.sdk.io;

import org.apache.beam.sdk.annotations.Experimental;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.joda.time.Instant;

import java.util.UUID;

/**
 * A {@link PTransform} that writes to a {@link Sink}. A write begins with a sequential global
 * initialization of a sink, followed by a parallel write, and ends with a sequential finalization
 * of the write. The output of a write is {@link PDone}.  In the case of an empty PCollection, only
 * the global initialization and finalization will be performed.
 *
 * <p>Currently, only batch workflows can contain Write transforms.
 *
 * <p>Example usage:
 *
 * <p>{@code p.apply(Write.to(new MySink(...)));}
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class Write {
  /**
   * Creates a Write transform that writes to the given Sink.
   */
  public static <T> Bound<T> to(Sink<T> sink) {
    return new Bound<>(sink);
  }

  /**
   * A {@link PTransform} that writes to a {@link Sink}. See {@link Write} and {@link Sink} for
   * documentation about writing to Sinks.
   */
  public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
    private final Sink<T> sink;

    private Bound(Sink<T> sink) {
      this.sink = sink;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      PipelineOptions options = input.getPipeline().getOptions();
      sink.validate(options);
      return createWrite(input, sink.createWriteOperation(options));
    }

    /**
     * Returns the {@link Sink} associated with this PTransform.
     */
    public Sink<T> getSink() {
      return sink;
    }

    /**
     * A write is performed as sequence of three {@link ParDo}'s.
     *
     * <p>In the first, a do-once ParDo is applied to a singleton PCollection containing the Sink's
     * {@link WriteOperation}. In this initialization ParDo, {@link WriteOperation#initialize} is
     * called. The output of this ParDo is a singleton PCollection
     * containing the WriteOperation.
     *
     * <p>This singleton collection containing the WriteOperation is then used as a side input to a
     * ParDo over the PCollection of elements to write. In this bundle-writing phase,
     * {@link WriteOperation#createWriter} is called to obtain a {@link Writer}.
     * {@link Writer#open} and {@link Writer#close} are called in {@link DoFn#startBundle} and
     * {@link DoFn#finishBundle}, respectively, and {@link Writer#write} method is called for every
     * element in the bundle. The output of this ParDo is a PCollection of <i>writer result</i>
     * objects (see {@link Sink} for a description of writer results)-one for each bundle.
     *
     * <p>The final do-once ParDo uses the singleton collection of the WriteOperation as input and
     * the collection of writer results as a side-input. In this ParDo,
     * {@link WriteOperation#finalize} is called to finalize the write.
     *
     * <p>If the write of any element in the PCollection fails, {@link Writer#close} will be called
     * before the exception that caused the write to fail is propagated and the write result will be
     * discarded.
     *
     * <p>Since the {@link WriteOperation} is serialized after the initialization ParDo and
     * deserialized in the bundle-writing and finalization phases, any state change to the
     * WriteOperation object that occurs during initialization is visible in the latter phases.
     * However, the WriteOperation is not serialized after the bundle-writing phase.  This is why
     * implementations should guarantee that {@link WriteOperation#createWriter} does not mutate
     * WriteOperation).
     */
    private <WriteT> PDone createWrite(
        PCollection<T> input, WriteOperation<T, WriteT> writeOperation) {
      Pipeline p = input.getPipeline();

      // A coder to use for the WriteOperation.
      @SuppressWarnings("unchecked")
      Coder<WriteOperation<T, WriteT>> operationCoder =
          (Coder<WriteOperation<T, WriteT>>) SerializableCoder.of(writeOperation.getClass());

      // A singleton collection of the WriteOperation, to be used as input to a ParDo to initialize
      // the sink.
      PCollection<WriteOperation<T, WriteT>> operationCollection =
          p.apply(Create.<WriteOperation<T, WriteT>>of(writeOperation).withCoder(operationCoder));

      // Initialize the resource in a do-once ParDo on the WriteOperation.
      operationCollection = operationCollection
          .apply("Initialize", ParDo.of(
              new DoFn<WriteOperation<T, WriteT>, WriteOperation<T, WriteT>>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
              WriteOperation<T, WriteT> writeOperation = c.element();
              writeOperation.initialize(c.getPipelineOptions());
              // The WriteOperation is also the output of this ParDo, so it can have mutable
              // state.
              c.output(writeOperation);
            }
          }))
          .setCoder(operationCoder);

      // Create a view of the WriteOperation to be used as a sideInput to the parallel write phase.
      final PCollectionView<WriteOperation<T, WriteT>> writeOperationView =
          operationCollection.apply(View.<WriteOperation<T, WriteT>>asSingleton());

      // Perform the per-bundle writes as a ParDo on the input PCollection (with the WriteOperation
      // as a side input) and collect the results of the writes in a PCollection.
      // There is a dependency between this ParDo and the first (the WriteOperation PCollection
      // as a side input), so this will happen after the initial ParDo.
      PCollection<WriteT> results = input
          .apply("WriteBundles", ParDo.of(new DoFn<T, WriteT>() {
            // Writer that will write the records in this bundle. Lazily
            // initialized in processElement.
            private Writer<T, WriteT> writer = null;

            @Override
            public void processElement(ProcessContext c) throws Exception {
              // Lazily initialize the Writer
              if (writer == null) {
                WriteOperation<T, WriteT> writeOperation = c.sideInput(writeOperationView);
                writer = writeOperation.createWriter(c.getPipelineOptions());
                writer.open(UUID.randomUUID().toString());
              }
              try {
                writer.write(c.element());
              } catch (Exception e) {
                // Discard write result and close the write.
                try {
                  writer.close();
                } catch (Exception closeException) {
                  // Do not mask the exception that caused the write to fail.
                }
                throw e;
              }
            }

            @Override
            public void finishBundle(Context c) throws Exception {
              if (writer != null) {
                WriteT result = writer.close();
                // Output the result of the write.
                c.outputWithTimestamp(result, Instant.now());
              }
            }
          }).withSideInputs(writeOperationView))
          .setCoder(writeOperation.getWriterResultCoder())
          .apply(Window.<WriteT>into(new GlobalWindows()));

      final PCollectionView<Iterable<WriteT>> resultsView =
          results.apply(View.<WriteT>asIterable());

      // Finalize the write in another do-once ParDo on the singleton collection containing the
      // Writer. The results from the per-bundle writes are given as an Iterable side input.
      // The WriteOperation's state is the same as after its initialization in the first do-once
      // ParDo. There is a dependency between this ParDo and the parallel write (the writer results
      // collection as a side input), so it will happen after the parallel write.
      @SuppressWarnings("unused")
      final PCollection<Integer> done = operationCollection
          .apply("Finalize", ParDo.of(new DoFn<WriteOperation<T, WriteT>, Integer>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
              Iterable<WriteT> results = c.sideInput(resultsView);
              WriteOperation<T, WriteT> writeOperation = c.element();
              writeOperation.finalize(results, c.getPipelineOptions());
            }
          }).withSideInputs(resultsView));
      return PDone.in(input.getPipeline());
    }
  }
}
