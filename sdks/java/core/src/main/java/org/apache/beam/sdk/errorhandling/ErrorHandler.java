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
package org.apache.beam.sdk.errorhandling;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Error Handler is a utility object used for plumbing error PCollections to a configured sink
 * Error Handlers must be closed before a pipeline is run to properly pipe error collections to the
 * sink, and the pipeline will be rejected if any handlers aren't closed.
 *
 * @param <X> The type of the error object. This will usually be a {@link BadRecord}, but can be any
 *     type
 * @param <T> The return type of the sink PTransform.
 *     <p>Usage of Error Handlers:
 *     <p>Simple usage with one DLQ
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (ErrorHandler<E,T> errorHandler = pipeline.registerErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withDeadLetterQueue(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 *     Usage with multiple DLQ stages
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (ErrorHandler<E,T> errorHandler = pipeline.registerErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withDeadLetterQueue(errorHandler))
 *                        .apply(OtherTransform.builder().withDeadLetterQueue(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 */
public interface ErrorHandler<X, T extends POutput> extends AutoCloseable {

  void addErrorCollection(PCollection<X> errorCollection);

  boolean isClosed();

  T getOutput();

  class PTransformErrorHandler<X, T extends POutput> implements ErrorHandler<X, T> {

    private static final Logger LOG = LoggerFactory.getLogger(PTransformErrorHandler.class);
    private final PTransform<PCollection<X>, T> sinkTransform;

    private final List<PCollection<X>> errorCollections = new ArrayList<>();

    @Nullable private T sinkOutput = null;

    private boolean closed = false;

    /**
     * Constructs a new ErrorHandler, but should not be called directly. Instead, call
     * pipeline.registerErrorHandler to ensure safe pipeline construction
     */
    @Internal
    public PTransformErrorHandler(PTransform<PCollection<X>, T> sinkTransform) {
      this.sinkTransform = sinkTransform;
    }

    @Override
    public void addErrorCollection(PCollection<X> errorCollection) {
      errorCollections.add(errorCollection);
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public T getOutput() {
      if (!this.isClosed()) {
        throw new IllegalStateException(
            "ErrorHandler must be finalized before the output can be returned");
      }
      // make the static analysis checker happy
      Preconditions.checkArgumentNotNull(sinkOutput);
      return sinkOutput;
    }

    @Override
    public void close() {
      closed = true;
      if (errorCollections.isEmpty()) {
        LOG.warn("Empty list of error pcollections passed to ErrorHandler.");
        return;
      }
      sinkOutput =
          PCollectionList.of(errorCollections).apply(Flatten.pCollections()).apply(sinkTransform);
    }
  }

  @Internal
  class NoOpErrorHandler<X, T extends POutput> implements ErrorHandler<X, T> {

    @Override
    public void addErrorCollection(PCollection<X> errorCollection) {}

    @Override
    public boolean isClosed() {
      throw new IllegalArgumentException("No Op handler should not be closed");
    }

    @Override
    public T getOutput() {
      throw new IllegalArgumentException("No Op handler has no output");
    }

    @Override
    public void close() {
      throw new IllegalArgumentException("No Op handler should not be closed");
    }
  }
}
