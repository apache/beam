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
package org.apache.beam.sdk.io.datadog;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DatadogIO} class provides a {@link PTransform} that allows writing {@link
 * DatadogEvent} messages into a Datadog Logs API end point.
 */
public class DatadogIO {

  private static final Logger LOG = LoggerFactory.getLogger(DatadogIO.class);

  private DatadogIO() {}

  public static Write.Builder writeBuilder() {
    return writeBuilder(null);
  }

  public static Write.Builder writeBuilder(@Nullable Integer minBatchCount) {
    return new AutoValue_DatadogIO_Write.Builder().setMinBatchCount(minBatchCount);
  }

  /**
   * Class {@link Write} provides a {@link PTransform} that allows writing {@link DatadogEvent}
   * records into a Datadog Logs API end-point using HTTP POST requests. In the event of an error, a
   * {@link PCollection} of {@link DatadogWriteError} records are returned for further processing or
   * storing into a deadletter sink.
   */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<DatadogEvent>, PCollection<DatadogWriteError>> {

    abstract String url();

    abstract String apiKey();

    @Nullable
    abstract Integer minBatchCount();

    @Nullable
    abstract Integer batchCount();

    @Nullable
    abstract Long maxBufferSize();

    @Nullable
    abstract Integer parallelism();

    @Override
    public PCollection<DatadogWriteError> expand(PCollection<DatadogEvent> input) {

      LOG.info("Configuring DatadogEventWriter.");
      DatadogEventWriter.Builder builder =
          DatadogEventWriter.newBuilder(minBatchCount())
              .withMaxBufferSize(maxBufferSize())
              .withUrl(url())
              .withInputBatchCount(batchCount())
              .withApiKey(apiKey());

      DatadogEventWriter writer = builder.build();
      LOG.info("DatadogEventWriter configured");

      // Return a PCollection<DatadogWriteError>
      return input
          .apply("Create KV pairs", CreateKeys.of(parallelism()))
          .apply("Write Datadog events", ParDo.of(writer))
          .setCoder(DatadogWriteErrorCoder.of());
    }

    /** A builder for creating {@link Write} objects. */
    @AutoValue.Builder
    public abstract static class Builder {

      abstract Builder setUrl(String url);

      abstract String url();

      abstract Builder setApiKey(String apiKey);

      abstract String apiKey();

      abstract Builder setMinBatchCount(@Nullable Integer minBatchCount);

      abstract Builder setBatchCount(Integer batchCount);

      abstract Builder setMaxBufferSize(Long maxBufferSize);

      abstract Builder setParallelism(Integer parallelism);

      abstract Write autoBuild();

      /**
       * Method to set the url for Logs API.
       *
       * @param url for Logs API
       * @return {@link Builder}
       */
      public Builder withUrl(String url) {
        checkArgument(url != null, "withURL(url) called with null input.");
        return setUrl(url);
      }

      /**
       * Method to set the API key for Logs API.
       *
       * @param apiKey API key for Logs API
       * @return {@link Builder}
       */
      public Builder withApiKey(String apiKey) {
        checkArgument(apiKey != null, "withApiKey(apiKey) called with null input.");
        return setApiKey(apiKey);
      }

      /**
       * Method to set the Batch Count.
       *
       * @param batchCount for batching post requests.
       * @return {@link Builder}
       */
      public Builder withBatchCount(Integer batchCount) {
        checkArgument(batchCount != null, "withBatchCount(batchCount) called with null input.");
        return setBatchCount(batchCount);
      }

      /**
       * Method to set the Max Buffer Size.
       *
       * @param maxBufferSize for batching post requests.
       * @return {@link Builder}
       */
      public Builder withMaxBufferSize(Long maxBufferSize) {
        checkArgument(
            maxBufferSize != null, "withMaxBufferSize(maxBufferSize) called with null input.");
        return setMaxBufferSize(maxBufferSize);
      }

      /**
       * Method to set the parallelism.
       *
       * @param parallelism for controlling the number of http client connections.
       * @return {@link Builder}
       */
      public Builder withParallelism(Integer parallelism) {
        checkArgument(parallelism != null, "withParallelism(parallelism) called with null input.");
        return setParallelism(parallelism);
      }

      public Write build() {
        checkNotNull(url(), "Logs API url is required.");
        checkNotNull(apiKey(), "API key is required.");

        return autoBuild();
      }
    }

    private static class CreateKeys
        extends PTransform<PCollection<DatadogEvent>, PCollection<KV<Integer, DatadogEvent>>> {

      private static final Integer DEFAULT_PARALLELISM = 1;

      @Nullable private Integer requestedKeys;

      private CreateKeys(@Nullable Integer requestedKeys) {
        this.requestedKeys = requestedKeys;
      }

      static CreateKeys of(@Nullable Integer requestedKeys) {
        return new CreateKeys(requestedKeys);
      }

      @Override
      public PCollection<KV<Integer, DatadogEvent>> expand(PCollection<DatadogEvent> input) {

        return input
            .apply("Inject Keys", ParDo.of(new CreateKeysFn(this.requestedKeys)))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of()));
      }

      private static class CreateKeysFn extends DoFn<DatadogEvent, KV<Integer, DatadogEvent>> {

        @Nullable private Integer specifiedParallelism;
        private Integer calculatedParallelism;

        CreateKeysFn(@Nullable Integer specifiedParallelism) {
          this.specifiedParallelism = specifiedParallelism;
          this.calculatedParallelism =
              MoreObjects.firstNonNull(specifiedParallelism, DEFAULT_PARALLELISM);
          LOG.info("Parallelism set to: {}", calculatedParallelism);
        }

        @Setup
        public void setup() {
          // Initialization is now in the constructor to satisfy static analysis.
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
          context.output(
              KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
        }
      }
    }
  }
}
