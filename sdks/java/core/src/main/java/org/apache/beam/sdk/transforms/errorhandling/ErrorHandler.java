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
package org.apache.beam.sdk.transforms.errorhandling;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Error Handler is a utility object used for plumbing error PCollections to a configured sink
 * Error Handlers must be closed before a pipeline is run to properly pipe error collections to the
 * sink, and the pipeline will be rejected if any handlers aren't closed.
 *
 * @param <ErrorT> The type of the error object. This will usually be a {@link BadRecord}, but can
 *     be any type
 * @param <OutputT> The return type of the sink PTransform.
 *     <p>Usage of Error Handlers:
 *     <p>Simple usage with one DLQ
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (BadRecordErrorHandler<T> errorHandler = pipeline.registerBadRecordErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withErrorHandler(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 *     Usage with multiple DLQ stages
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (BadRecordErrorHandler<T> errorHandler = pipeline.registerBadRecordErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withErrorHandler(errorHandler))
 *                        .apply(OtherTransform.builder().withErrorHandler(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 *     This is marked as serializable despite never being needed on the runner, to enable it to be a
 *     parameter of an Autovalue configured PTransform.
 */
public interface ErrorHandler<ErrorT, OutputT extends POutput> extends AutoCloseable, Serializable {

  void addErrorCollection(PCollection<ErrorT> errorCollection);

  boolean isClosed();

  @Nullable
  OutputT getOutput();

  class PTransformErrorHandler<ErrorT, OutputT extends POutput>
      implements ErrorHandler<ErrorT, OutputT> {

    private static final Logger LOG = LoggerFactory.getLogger(PTransformErrorHandler.class);
    private final PTransform<PCollection<ErrorT>, OutputT> sinkTransform;

    // transient as Pipelines are not serializable
    private final transient Pipeline pipeline;

    private final Coder<ErrorT> coder;

    // transient as PCollections are not serializable
    private transient List<PCollection<ErrorT>> errorCollections = new ArrayList<>();

    // transient as PCollections are not serializable
    private transient @Nullable OutputT sinkOutput = null;

    private boolean closed = false;

    /**
     * Constructs a new ErrorHandler, but should not be called directly. Instead, call
     * pipeline.registerErrorHandler to ensure safe pipeline construction
     */
    @Internal
    public PTransformErrorHandler(
        PTransform<PCollection<ErrorT>, OutputT> sinkTransform,
        Pipeline pipeline,
        Coder<ErrorT> coder) {
      this.sinkTransform = sinkTransform;
      this.pipeline = pipeline;
      this.coder = coder;
    }

    private void readObject(ObjectInputStream aInputStream)
        throws ClassNotFoundException, IOException {
      aInputStream.defaultReadObject();
      errorCollections = new ArrayList<>();
    }

    @Override
    public void addErrorCollection(PCollection<ErrorT> errorCollection) {
      if (isClosed()) {
        throw new IllegalStateException(
            "Error collections cannot be added after Error Handler is closed");
      }
      errorCollections.add(errorCollection);
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public @Nullable OutputT getOutput() {
      if (!this.isClosed()) {
        throw new IllegalStateException(
            "ErrorHandler must be finalized before the output can be returned");
      }
      return sinkOutput;
    }

    @Override
    public void close() {
      if (closed) {
        throw new IllegalStateException(
            "Error handler is already closed, and may not be closed twice");
      }
      closed = true;
      PCollection<ErrorT> flattened;
      if (errorCollections.isEmpty()) {
        LOG.info("Empty list of error pcollections passed to ErrorHandler.");
        flattened = pipeline.apply(Create.empty(coder));
      } else {
        flattened = PCollectionList.of(errorCollections).apply(Flatten.pCollections());
      }
      LOG.debug(
          "{} error collections are being sent to {}",
          errorCollections.size(),
          sinkTransform.getName());
      String sinkTransformName = sinkTransform.getName();
      sinkOutput =
          flattened
              .apply(
                  "Record Error Metrics to " + sinkTransformName,
                  new WriteErrorMetrics<ErrorT>(sinkTransformName))
              .apply(
                  "Write to error Sink",
                  sinkTransform.addAnnotation(
                      "FeatureMetric", "ErrorHandler".getBytes(StandardCharsets.UTF_8)));
    }

    public static class WriteErrorMetrics<ErrorT>
        extends PTransform<PCollection<ErrorT>, PCollection<ErrorT>> {

      private final Counter errorCounter;

      public WriteErrorMetrics(String sinkTransformName) {
        errorCounter = Metrics.counter("ErrorMetrics", sinkTransformName + "-input");
      }

      @Override
      public PCollection<ErrorT> expand(PCollection<ErrorT> input) {
        return input.apply(ParDo.of(new CountErrors<ErrorT>(errorCounter)));
      }

      public static class CountErrors<ErrorT> extends DoFn<ErrorT, ErrorT> {

        private final Counter errorCounter;

        public CountErrors(Counter errorCounter) {
          this.errorCounter = errorCounter;
        }

        @ProcessElement
        public void processElement(@Element ErrorT error, OutputReceiver<ErrorT> receiver) {
          errorCounter.inc();
          receiver.output(error);
        }
      }
    }
  }

  class BadRecordErrorHandler<OutputT extends POutput>
      extends PTransformErrorHandler<BadRecord, OutputT> {

    /** Constructs a new ErrorHandler for handling BadRecords. */
    @Internal
    public BadRecordErrorHandler(
        PTransform<PCollection<BadRecord>, OutputT> sinkTransform, Pipeline pipeline) {
      super(sinkTransform, pipeline, BadRecord.getCoder(pipeline));
    }
  }

  /**
   * A default, placeholder error handler that exists to allow usage of .addErrorCollection()
   * without effects. This enables more simple codepaths without checking for whether the user
   * configured an error handler or not.
   */
  @Internal
  class DefaultErrorHandler<ErrorT, OutputT extends POutput>
      implements ErrorHandler<ErrorT, OutputT> {

    @Override
    public void addErrorCollection(PCollection<ErrorT> errorCollection) {}

    @Override
    public boolean isClosed() {
      throw new IllegalArgumentException(
          "No Op handler should not be closed. This implies this IO is misconfigured.");
    }

    @Override
    public @Nullable OutputT getOutput() {
      throw new IllegalArgumentException(
          "No Op handler has no output. This implies this IO is misconfigured.");
    }

    @Override
    public void close() {
      throw new IllegalArgumentException(
          "No Op handler should not be closed. This implies this IO is misconfigured.");
    }
  }
}
