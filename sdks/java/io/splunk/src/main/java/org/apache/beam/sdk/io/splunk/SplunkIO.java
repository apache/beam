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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded sink for Splunk's Http Event Collector (HEC).
 *
 * <p>For more information, see the online documentation at <a
 * href="https://dev.splunk.com/enterprise/docs/dataapps/httpeventcollector/">Splunk HEC</a>.
 *
 * <h3>Writing to Splunk's HEC</h3>
 *
 * <p>The {@link SplunkIO} class provides a {@link PTransform} that allows writing {@link
 * SplunkEvent} messages into a Splunk HEC end point.
 *
 * <p>It takes as an input a {@link PCollection PCollection&lt;SplunkEvent&gt;}, where each {@link
 * SplunkEvent} represents an event to be published to HEC.
 *
 * <p>To configure a {@link SplunkIO}, you must provide at a minimum:
 *
 * <ul>
 *   <li>url - HEC endpoint URL.
 *   <li>token - HEC endpoint token.
 * </ul>
 *
 * <p>The {@link SplunkIO} transform can be customized further by optionally specifying:
 *
 * <ul>
 *   <li>parallelism - Number of parallel requests to the HEC.
 *   <li>batchCount - Number of events in a single batch.
 *   <li>disableCertificateValidation - Whether to disable ssl validation (useful for self-signed
 *       certificates)
 *   <li>enableBatchLogs - Whether to enable batch logs.
 *   <li>enableGzipHttpCompression - Whether HTTP requests sent to Splunk HEC should be GZIP
 *       encoded.
 * </ul>
 *
 * <p>This transform will return any non-transient write failures via a {@link PCollection
 * PCollection&lt;SplunkWriteError&gt;}, where each {@link SplunkWriteError} captures the error that
 * occurred while attempting to write to HEC. These can be published to a dead-letter sink or
 * reprocessed.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<SplunkEvent> events = ...;
 *
 * PCollection<SplunkWriteError> errors =
 *         events.apply("WriteToSplunk",
 *              SplunkIO.write(url, token)
 *                  .withBatchCount(batchCount)
 *                  .withParallelism(parallelism)
 *                  .withDisableCertificateValidation(true));
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SplunkIO {

  /**
   * Write to Splunk's Http Event Collector (HEC).
   *
   * @param url splunk hec url
   * @param token splunk hec authentication token
   */
  public static Write write(String url, String token) {
    checkNotNull(url, "url is required.");
    checkNotNull(token, "token is required.");
    return write(StaticValueProvider.of(url), StaticValueProvider.of(token));
  }

  /**
   * Same as {@link SplunkIO#write(String, String)} but with {@link ValueProvider}.
   *
   * @param url splunk hec url
   * @param token splunk hec authentication token
   */
  public static Write write(ValueProvider<String> url, ValueProvider<String> token) {
    checkNotNull(url, "url is required.");
    checkNotNull(token, "token is required.");
    return new AutoValue_SplunkIO_Write.Builder().setUrl(url).setToken(token).build();
  }

  private static final Logger LOG = LoggerFactory.getLogger(SplunkIO.class);

  private SplunkIO() {}

  /**
   * Class {@link Write} provides a {@link PTransform} that allows writing {@link SplunkEvent}
   * records into a Splunk HTTP Event Collector end-point using HTTP POST requests.
   *
   * <p>In the event of an error, a {@link PCollection PCollection&lt;SplunkWriteError&gt;} is
   * returned for further processing or storing into a dead-letter sink.
   */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<SplunkEvent>, PCollection<SplunkWriteError>> {

    abstract @Nullable ValueProvider<String> url();

    abstract @Nullable ValueProvider<String> token();

    abstract @Nullable ValueProvider<Integer> batchCount();

    abstract @Nullable ValueProvider<Integer> parallelism();

    abstract @Nullable ValueProvider<Boolean> disableCertificateValidation();

    abstract @Nullable ValueProvider<String> rootCaCertificatePath();

    abstract @Nullable ValueProvider<Boolean> enableBatchLogs();

    abstract @Nullable ValueProvider<Boolean> enableGzipHttpCompression();

    abstract Builder toBuilder();

    @Override
    public PCollection<SplunkWriteError> expand(PCollection<SplunkEvent> input) {

      LOG.info("Configuring SplunkEventWriter.");
      SplunkEventWriter.Builder builder =
          SplunkEventWriter.newBuilder()
              .withUrl(url())
              .withInputBatchCount(batchCount())
              .withDisableCertificateValidation(disableCertificateValidation())
              .withToken(token())
              .withRootCaCertificatePath(rootCaCertificatePath())
              .withEnableBatchLogs(enableBatchLogs())
              .withEnableGzipHttpCompression(enableGzipHttpCompression());

      SplunkEventWriter writer = builder.build();
      LOG.info("SplunkEventWriter configured");

      // Return a PCollection<SplunkWriteError>
      return input
          .apply("Create KV pairs", CreateKeys.of(parallelism()))
          .apply("Write Splunk events", ParDo.of(writer));
    }

    /** A builder for creating {@link Write} objects. */
    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setToken(ValueProvider<String> token);

      abstract Builder setBatchCount(ValueProvider<Integer> batchCount);

      abstract Builder setParallelism(ValueProvider<Integer> parallelism);

      abstract Builder setDisableCertificateValidation(
          ValueProvider<Boolean> disableCertificateValidation);

      abstract Builder setRootCaCertificatePath(ValueProvider<String> rootCaCertificatePath);

      abstract Builder setEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs);

      abstract Builder setEnableGzipHttpCompression(
          ValueProvider<Boolean> enableGzipHttpCompression);

      abstract Write build();
    }

    /**
     * Same as {@link SplunkIO.Write#withBatchCount(Integer)} but with {@link ValueProvider}.
     *
     * @param batchCount for batching requests
     */
    public Write withBatchCount(ValueProvider<Integer> batchCount) {
      return toBuilder().setBatchCount(batchCount).build();
    }

    /**
     * Sets batchCount for sending multiple events in a single request to the HEC.
     *
     * @param batchCount for batching requests
     */
    public Write withBatchCount(Integer batchCount) {
      return toBuilder().setBatchCount(StaticValueProvider.of(batchCount)).build();
    }

    /**
     * Same as {@link SplunkIO.Write#withBatchCount(Integer)} but with {@link ValueProvider}.
     *
     * @param parallelism for controlling the number of concurrent http client connections
     */
    public Write withParallelism(ValueProvider<Integer> parallelism) {
      return toBuilder().setParallelism(parallelism).build();
    }

    /**
     * Sets the number of parallel http client connections to the HEC.
     *
     * @param parallelism controlling the number of http client connections
     * @return {@link Builder}
     */
    public Write withParallelism(Integer parallelism) {
      return toBuilder().setParallelism(StaticValueProvider.of(parallelism)).build();
    }

    /**
     * Same as {@link SplunkIO.Write#withDisableCertificateValidation(Boolean)} but with {@link
     * ValueProvider}.
     *
     * @param disableCertificateValidation for disabling certificate validation
     * @return {@link Builder}
     */
    public Write withDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation) {
      return toBuilder().setDisableCertificateValidation(disableCertificateValidation).build();
    }

    /**
     * Disables ssl certificate validation.
     *
     * @param disableCertificateValidation for disabling certificate validation
     */
    public Write withDisableCertificateValidation(Boolean disableCertificateValidation) {
      return toBuilder()
          .setDisableCertificateValidation(StaticValueProvider.of(disableCertificateValidation))
          .build();
    }

    /**
     * Same as {@link Builder#withRootCaCertificatePath(ValueProvider)} but without a {@link
     * ValueProvider}.
     *
     * @param rootCaCertificatePath Path to root CA certificate
     * @return {@link Builder}
     */
    public Write withRootCaCertificatePath(ValueProvider<String> rootCaCertificatePath) {
      return toBuilder().setRootCaCertificatePath(rootCaCertificatePath).build();
    }

    /**
     * Method to set the root CA certificate.
     *
     * @param rootCaCertificatePath Path to root CA certificate
     * @return {@link Builder}
     */
    public Write withRootCaCertificatePath(String rootCaCertificatePath) {
      return toBuilder()
          .setRootCaCertificatePath(StaticValueProvider.of(rootCaCertificatePath))
          .build();
    }

    /**
     * Same as {@link Builder#withEnableBatchLogs(ValueProvider)} but without a {@link
     * ValueProvider}.
     *
     * @param enableBatchLogs whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Write withEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs) {
      return toBuilder().setEnableBatchLogs(enableBatchLogs).build();
    }

    /**
     * Method to enable batch logs.
     *
     * @param enableBatchLogs whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Write withEnableBatchLogs(Boolean enableBatchLogs) {
      return toBuilder().setEnableBatchLogs(StaticValueProvider.of(enableBatchLogs)).build();
    }

    /**
     * Same as {@link Builder#withEnableGzipHttpCompression(ValueProvider)} but without a {@link
     * ValueProvider}.
     *
     * @param enableGzipHttpCompression whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Write withEnableGzipHttpCompression(ValueProvider<Boolean> enableGzipHttpCompression) {
      return toBuilder().setEnableGzipHttpCompression(enableGzipHttpCompression).build();
    }

    /**
     * Method to specify if HTTP requests sent to Splunk should be GZIP encoded.
     *
     * @param enableGzipHttpCompression whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Write withEnableGzipHttpCompression(Boolean enableGzipHttpCompression) {
      return toBuilder()
          .setEnableGzipHttpCompression(StaticValueProvider.of(enableGzipHttpCompression))
          .build();
    }

    /**
     * Provides synthetic keys that are used to control the number of parallel requests towards the
     * Splunk HEC endpoint.
     */
    private static class CreateKeys
        extends PTransform<PCollection<SplunkEvent>, PCollection<KV<Integer, SplunkEvent>>> {

      private static final Integer DEFAULT_PARALLELISM = 1;

      private ValueProvider<Integer> requestedKeys;

      private CreateKeys(ValueProvider<Integer> requestedKeys) {
        this.requestedKeys = requestedKeys;
      }

      static CreateKeys of(ValueProvider<Integer> requestedKeys) {
        return new CreateKeys(requestedKeys);
      }

      @Override
      public PCollection<KV<Integer, SplunkEvent>> expand(PCollection<SplunkEvent> input) {

        return input.apply("Inject Keys", ParDo.of(new CreateKeysFn(this.requestedKeys)));
      }

      private static class CreateKeysFn extends DoFn<SplunkEvent, KV<Integer, SplunkEvent>> {

        private ValueProvider<Integer> specifiedParallelism;
        private Integer calculatedParallelism;

        CreateKeysFn(ValueProvider<Integer> specifiedParallelism) {
          this.specifiedParallelism = specifiedParallelism;
        }

        @Setup
        public void setup() {

          if (calculatedParallelism == null) {

            if (specifiedParallelism != null) {
              calculatedParallelism = specifiedParallelism.get();
            }

            calculatedParallelism =
                MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

            LOG.info("Parallelism set to: {}", calculatedParallelism);
          }
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
