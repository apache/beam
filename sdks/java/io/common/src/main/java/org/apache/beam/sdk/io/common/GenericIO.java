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
package org.apache.beam.sdk.io.common;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.UUID;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GenericIO {

  public static <InputT, OutputT> Write<InputT, OutputT> write() {
    return Write.<InputT, OutputT>create();
  }

  @AutoValue
  public abstract static class Write<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    static <InputT, OutputT> Write<InputT, OutputT> create() {
      return new AutoValue_GenericIO_Write();
    }

    abstract BeamIOClientFactory<InputT, OutputT> getClientFactory();

    abstract Builder<InputT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT, OutputT> {
      abstract Builder<InputT, OutputT> setClientFactory(BeamIOClientFactory<InputT, OutputT> factory);
      abstract Write<InputT, OutputT> build();
    }

    public Write<InputT, OutputT> withClientFactory(BeamIOClientFactory<InputT, OutputT> clientFactory) {
      return toBuilder().setClientFactory(clientFactory).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      return null;
    }
  }

  /**
   * A factory for a BeamIOClient. This is implemented by the user, and
   * passed as a parameter at runtime.
   */
  public interface BeamIOClientFactory<ElementT, ResultT> extends Serializable {
    BeamIOClient<ElementT, ResultT> connection();
  }

  /**
   * TODO
   */
  public interface BeamIOClient<ElementT, ResultT> {
    /**
     * Note: The ordering of elements in the input iterable is guaranteed to be preserved
     *           on retries.
     */
    public Iterable<ResultT> startBatch(@Nullable String destination, Iterable<ElementT> elements, UUID batchId) throws BeamIOException;

    /**
     * Finalize the execution of this batch: Commit or
     */
    // TODO(pabloem): Figure out if we want this result type for this function or not.
    public Iterable<ResultT> finishBatch(UUID batchId) throws BeamIOException;
  }

  public static class BeamIOException extends Exception {
    public enum Type {
      THROTTLED,
      RETRIABLE_EXCEPTION,
      PERMANENT_EXCEPTION
    }
  }

}
