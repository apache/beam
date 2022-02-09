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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

public class GenericIO {

  public final static Integer DEFAULT_BATCH_SIZE = 1000;

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
    abstract long getElementsPerPeriod();
    abstract Duration getPeriod();
    abstract SerializableFunction<InputT, String> getDestinationFn();

    abstract Builder<InputT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT, OutputT> {
      abstract Builder<InputT, OutputT> setClientFactory(BeamIOClientFactory<InputT, OutputT> factory);
      abstract Builder<InputT, OutputT> setElementsPerPeriod(long elements);
      abstract Builder<InputT, OutputT> setPeriod(Duration period);
      abstract Builder<InputT, OutputT> setDestinationFn(SerializableFunction<InputT, String> destinationFn);
      abstract Write<InputT, OutputT> build();
    }

    /** Specifies the maximum throughput for the transform. */
    public Write<InputT, OutputT> withMaxThroughput(long numElements, Duration periodLength) {
      checkArgument(
          numElements > 0,
          "Number of elements in withMaxThroughput must be positive, but was: %s",
          numElements);
      checkArgument(periodLength != null, "periodLength can not be null");
      return toBuilder().setElementsPerPeriod(numElements).setPeriod(periodLength).build();
    }

    public Write<InputT, OutputT> withClientFactory(BeamIOClientFactory<InputT, OutputT> clientFactory) {
      return toBuilder().setClientFactory(clientFactory).build();
    }

    public Write<InputT, OutputT> withDestination(String destination) {
      return toBuilder().setDestinationFn(elm -> destination).build();
    }

    public Write<InputT, OutputT> withDestination(SerializableFunction<InputT, String> destinationFn) {
      return toBuilder().setDestinationFn(destinationFn).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      input
          .apply(MapElements.via(new SimpleFunction<InputT, KV<String, InputT>>() {
        @Override
        public KV<String, InputT> apply(InputT input) {
          return KV.of(getDestinationFn().apply(input), input);
        }
      }))
          .apply(GroupIntoBatches.<String, InputT>ofSize(DEFAULT_BATCH_SIZE).withShardedKey())
          // TODO(pabloem) Need a way to produce a stable UUID.
          .apply(ParDo.of(new WriteFn<InputT, OutputT>(getClientFactory())));
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
    private final Type type;
    private final Throwable e;
    public Type getType() {
      return type;
    }
    public Throwable getUnderlyingException() {
      return e;
    }
    BeamIOException(Type type, Throwable e) {
      this.type = type;
      this.e = e;
    }
    public enum Type {
      THROTTLED,
      RETRIABLE_EXCEPTION,
      PERMANENT_EXCEPTION
    }
  }

  private static class WriteFn<InputT, OutputT> extends DoFn<KV<ShardedKey<String>, Iterable<InputT>>, OutputT> {
    private final BeamIOClientFactory<InputT, OutputT> clientFactory;
    private transient BeamIOClient<InputT, OutputT> client;

    WriteFn(BeamIOClientFactory<InputT, OutputT> clientFactory) {
      this.clientFactory = clientFactory;
    }

    @DoFn.ProcessElement
    public void process(@DoFn.Element KV<ShardedKey<String>, Iterable<InputT>> elm,
        OutputReceiver<OutputT> receiver) {
      if (this.client == null) {
        this.client = this.clientFactory.connection();
      }
      try {
        this.client.startBatch(elm.getKey().getKey(), elm.getValue(), UUID.randomUUID());
      } catch (BeamIOException e) {
        switch (e.getType()) {
          case THROTTLED:
            break;
          case PERMANENT_EXCEPTION:
            break;
          case RETRIABLE_EXCEPTION:
            break;
          default:
            break;
        }
      }
    }
  }

}
