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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.InetAddresses;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.InternetDomainName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} to write {@link SplunkEvent}s to Splunk's HEC endpoint. */
@AutoValue
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
abstract class SplunkEventWriter extends DoFn<KV<Integer, SplunkEvent>, SplunkWriteError> {

  private static final Integer DEFAULT_BATCH_COUNT = 10;
  private static final Boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  private static final Boolean DEFAULT_ENABLE_BATCH_LOGS = true;
  private static final Boolean DEFAULT_ENABLE_GZIP_HTTP_COMPRESSION = true;
  private static final Logger LOG = LoggerFactory.getLogger(SplunkEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Counter INPUT_COUNTER =
      Metrics.counter(SplunkEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-failed-events");
  private static final Counter INVALID_REQUESTS =
      Metrics.counter(SplunkEventWriter.class, "http-invalid-requests");
  private static final Counter SERVER_ERROR_REQUESTS =
      Metrics.counter(SplunkEventWriter.class, "http-server-error-requests");
  private static final Counter VALID_REQUESTS =
      Metrics.counter(SplunkEventWriter.class, "http-valid-requests");
  private static final Distribution SUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(SplunkEventWriter.class, "successful_write_to_splunk_latency_ms");
  private static final Distribution UNSUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(SplunkEventWriter.class, "unsuccessful_write_to_splunk_latency_ms");
  private static final Distribution SUCCESSFUL_WRITE_BATCH_SIZE =
      Metrics.distribution(SplunkEventWriter.class, "write_to_splunk_batch");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";

  private static final Pattern URL_PATTERN = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");

  @VisibleForTesting
  protected static final String INVALID_URL_FORMAT_MESSAGE =
      "Invalid url format. Url format should match PROTOCOL://HOST[:PORT], where PORT is optional. "
          + "Supported Protocols are http and https. eg: http://hostname:8088";

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<SplunkEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private Integer batchCount;
  private Boolean disableValidation;
  private HttpEventPublisher publisher;
  private Boolean enableBatchLogs;
  private Boolean enableGzipHttpCompression;

  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  /** A builder class for creating a {@link SplunkEventWriter}. */
  static Builder newBuilder() {
    return new AutoValue_SplunkEventWriter.Builder();
  }

  abstract @Nullable ValueProvider<String> url();

  abstract @Nullable ValueProvider<String> token();

  abstract @Nullable ValueProvider<Boolean> disableCertificateValidation();

  abstract @Nullable ValueProvider<Integer> inputBatchCount();

  abstract @Nullable ValueProvider<String> rootCaCertificatePath();

  abstract @Nullable ValueProvider<Boolean> enableBatchLogs();

  abstract @Nullable ValueProvider<Boolean> enableGzipHttpCompression();

  @Setup
  public void setup() {

    checkArgument(url().isAccessible(), "url is required for writing events.");
    checkArgument(isValidUrlFormat(url().get()), INVALID_URL_FORMAT_MESSAGE);
    checkArgument(token().isAccessible(), "Access token is required for writing events.");

    // Either user supplied or default batchCount.
    if (batchCount == null) {

      if (inputBatchCount() != null) {
        batchCount = inputBatchCount().get();
      }

      batchCount = MoreObjects.firstNonNull(batchCount, DEFAULT_BATCH_COUNT);
      LOG.info("Batch count set to: {}", batchCount);
    }

    // Either user supplied or default disableValidation.
    if (disableValidation == null) {

      if (disableCertificateValidation() != null) {
        disableValidation = disableCertificateValidation().get();
      }

      disableValidation =
          MoreObjects.firstNonNull(disableValidation, DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
      LOG.info("Disable certificate validation set to: {}", disableValidation);
    }

    // Either user supplied or default enableBatchLogs.
    if (enableBatchLogs == null) {

      if (enableBatchLogs() != null) {
        enableBatchLogs = enableBatchLogs().get();
      }

      enableBatchLogs = MoreObjects.firstNonNull(enableBatchLogs, DEFAULT_ENABLE_BATCH_LOGS);
      LOG.info("Enable Batch logs set to: {}", enableBatchLogs);
    }

    // Either user supplied or default enableGzipHttpCompression.
    if (enableGzipHttpCompression == null) {

      if (enableGzipHttpCompression() != null) {
        enableGzipHttpCompression = enableGzipHttpCompression().get();
      }

      enableGzipHttpCompression =
          MoreObjects.firstNonNull(enableGzipHttpCompression, DEFAULT_ENABLE_GZIP_HTTP_COMPRESSION);
      LOG.info("Enable gzip http compression set to: {}", enableGzipHttpCompression);
    }

    try {
      HttpEventPublisher.Builder builder =
          HttpEventPublisher.newBuilder()
              .withUrl(url().get())
              .withToken(token().get())
              .withDisableCertificateValidation(disableValidation)
              .withEnableGzipHttpCompression(enableGzipHttpCompression);

      if (rootCaCertificatePath() != null && rootCaCertificatePath().get() != null) {
        builder.withRootCaCertificate(getCertFromGcsAsBytes(rootCaCertificatePath().get()));
      }

      publisher = builder.build();
      LOG.info("Successfully created HttpEventPublisher");

    } catch (NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | IOException
        | CertificateException e) {
      LOG.error("Error creating HttpEventPublisher: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, SplunkEvent> input,
      OutputReceiver<SplunkWriteError> receiver,
      BoundedWindow window,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @TimerId(TIME_ID_NAME) Timer timer)
      throws IOException {

    Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    SplunkEvent event = input.getValue();
    INPUT_COUNTER.inc();
    bufferState.add(event);
    count += 1;
    countState.write(count);
    timer.offset(Duration.standardSeconds(DEFAULT_FLUSH_DELAY)).setRelative();

    if (count >= batchCount) {
      if (enableBatchLogs) {
        LOG.info("Flushing batch of {} events", count);
      }
      flush(receiver, bufferState, countState);
    }
  }

  @OnTimer(TIME_ID_NAME)
  public void onExpiry(
      OutputReceiver<SplunkWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<SplunkEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState)
      throws IOException {

    if (MoreObjects.<Long>firstNonNull(countState.read(), 0L) > 0) {
      if (enableBatchLogs) {
        LOG.info("Flushing window with {} events", countState.read());
      }
      flush(receiver, bufferState, countState);
    }
  }

  @Teardown
  public void tearDown() {
    if (this.publisher != null) {
      try {
        this.publisher.close();
        LOG.info("Successfully closed HttpEventPublisher");

      } catch (IOException e) {
        LOG.warn("Received exception while closing HttpEventPublisher: {}", e.getMessage());
      }
    }
  }

  /**
   * Flushes a batch of requests via {@link HttpEventPublisher}.
   *
   * @param receiver Receiver to write {@link SplunkWriteError}s to
   */
  private void flush(
      OutputReceiver<SplunkWriteError> receiver,
      BagState<SplunkEvent> bufferState,
      ValueState<Long> countState)
      throws IOException {

    if (!bufferState.isEmpty().read()) {

      HttpResponse response = null;
      List<SplunkEvent> events = Lists.newArrayList(bufferState.read());
      long startTime = System.nanoTime();
      try {
        // Important to close this response to avoid connection leak.
        response = publisher.execute(events);

        if (!response.isSuccessStatusCode()) {
          UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          FAILED_WRITES.inc(countState.read());
          int statusCode = response.getStatusCode();
          if (statusCode >= 400 && statusCode < 500) {
            INVALID_REQUESTS.inc();
          } else if (statusCode >= 500 && statusCode < 600) {
            SERVER_ERROR_REQUESTS.inc();
          }

          logWriteFailures(
              countState,
              response.getStatusCode(),
              response.parseAsString(),
              response.getStatusMessage());
          flushWriteFailures(
              events, response.getStatusMessage(), response.getStatusCode(), receiver);

        } else {
          SUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          SUCCESS_WRITES.inc(countState.read());
          VALID_REQUESTS.inc();
          SUCCESSFUL_WRITE_BATCH_SIZE.update(countState.read());

          if (enableBatchLogs) {
            LOG.info("Successfully wrote {} events", countState.read());
          }
        }

      } catch (HttpResponseException e) {
        LOG.error(
            "Error writing to Splunk. StatusCode: {}, content: {}, StatusMessage: {}",
            e.getStatusCode(),
            e.getContent(),
            e.getStatusMessage());
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(countState.read());
        int statusCode = e.getStatusCode();
        if (statusCode >= 400 && statusCode < 500) {
          INVALID_REQUESTS.inc();
        } else if (statusCode >= 500 && statusCode < 600) {
          SERVER_ERROR_REQUESTS.inc();
        }

        logWriteFailures(countState, e.getStatusCode(), e.getContent(), e.getStatusMessage());

        flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

      } catch (IOException ioe) {
        LOG.error("Error writing to Splunk: {}", ioe.getMessage());
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(countState.read());
        INVALID_REQUESTS.inc();

        logWriteFailures(countState, 0, ioe.getMessage(), null);

        flushWriteFailures(events, ioe.getMessage(), null, receiver);

      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed events to an output PCollection.
        bufferState.clear();
        countState.clear();

        // We've observed cases where errors at this point can cause the pipeline to keep retrying
        // the same events over and over (e.g. from Dataflow Runner's Pub/Sub implementation). Since
        // the events have either been published or wrapped for error handling, we can safely
        // ignore this error, though there may or may not be a leak of some type depending on
        // HttpResponse's implementation. However, any potential leak would still happen if we let
        // the exception fall through, so this isn't considered a major issue.
        try {
          if (response != null) {
            response.ignore();
          }
        } catch (IOException e) {
          LOG.warn(
              "Error ignoring response from Splunk. Messages should still have published, but there"
                  + " might be a connection leak.",
              e);
        }
      }
    }
  }

  /** Utility method to log write failures. */
  private void logWriteFailures(
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      int statusCode,
      String content,
      String statusMessage) {
    if (enableBatchLogs) {
      LOG.error("Failed to write {} events", countState.read());
    }
    LOG.error(
        "Error writing to Splunk. StatusCode: {}, content: {}, StatusMessage: {}",
        statusCode,
        content,
        statusMessage);
  }

  /**
   * Flushes failed write events.
   *
   * <p>This will also un-batch events if batching was used to write records.
   *
   * @param events list of {@link SplunkEvent}s to un-batch
   * @param statusMessage status message to be added to {@link SplunkWriteError}
   * @param statusCode status code to be added to {@link SplunkWriteError}
   * @param receiver receiver to write {@link SplunkWriteError}s to
   */
  private static void flushWriteFailures(
      List<SplunkEvent> events,
      String statusMessage,
      Integer statusCode,
      OutputReceiver<SplunkWriteError> receiver) {

    checkNotNull(events, "events cannot be null.");

    SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();

    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }

    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }

    for (SplunkEvent event : events) {
      String payload = GSON.toJson(event);
      SplunkWriteError error = builder.withPayload(payload).create();

      receiver.output(error);
    }
  }

  /**
   * Reads a root CA certificate from GCS and returns it as raw bytes.
   *
   * @param filePath path to root CA cert in GCS
   * @return raw contents of cert
   * @throws RuntimeException thrown if not able to read or parse cert
   */
  public static byte[] getCertFromGcsAsBytes(String filePath) throws IOException {
    MatchResult.Metadata fileMetadata = FileSystems.matchSingleFileSpec(filePath);
    ReadableByteChannel channel = FileSystems.open(fileMetadata.resourceId());
    try (InputStream inputStream = Channels.newInputStream(channel)) {
      return IOUtils.toByteArray(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error when reading: " + filePath, e);
    }
  }

  @VisibleForTesting
  static boolean isValidUrlFormat(String url) {
    Matcher matcher = URL_PATTERN.matcher(url);
    if (matcher.find()) {
      String host = matcher.group(2);
      return InetAddresses.isInetAddress(host) || InternetDomainName.isValid(host);
    }
    return false;
  }

  /**
   * Converts Nanoseconds to Milliseconds.
   *
   * @param ns time in nanoseconds
   * @return time in milliseconds
   */
  private static long nanosToMillis(long ns) {
    return Math.round(((double) ns) / 1e6);
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setUrl(ValueProvider<String> url);

    abstract ValueProvider<String> url();

    abstract Builder setToken(ValueProvider<String> token);

    abstract ValueProvider<String> token();

    abstract Builder setDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation);

    abstract Builder setRootCaCertificatePath(ValueProvider<String> rootCaCertificatePath);

    abstract Builder setEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs);

    abstract Builder setEnableGzipHttpCompression(ValueProvider<Boolean> enableGzipHttpCompression);

    abstract Builder setInputBatchCount(ValueProvider<Integer> inputBatchCount);

    abstract SplunkEventWriter autoBuild();

    /**
     * Sets the url for HEC event collector.
     *
     * @param url for HEC event collector
     * @return {@link Builder}
     */
    Builder withUrl(ValueProvider<String> url) {
      checkArgument(url != null, "withURL(url) called with null input.");
      if (url.isAccessible()) {
        checkArgument(isValidUrlFormat(url.get()), INVALID_URL_FORMAT_MESSAGE);
      }
      return setUrl(url);
    }

    /**
     * Same as {@link Builder#withUrl(ValueProvider)} but without {@link ValueProvider}.
     *
     * @param url for HEC event collector
     * @return {@link Builder}
     */
    Builder withUrl(String url) {
      checkArgument(url != null, "withURL(url) called with null input.");
      checkArgument(isValidUrlFormat(url), INVALID_URL_FORMAT_MESSAGE);
      return setUrl(ValueProvider.StaticValueProvider.of(url));
    }

    /**
     * Sets the authentication token for HEC.
     *
     * @param token Authentication token for HEC event collector
     * @return {@link Builder}
     */
    Builder withToken(ValueProvider<String> token) {
      checkArgument(token != null, "withToken(token) called with null input.");
      return setToken(token);
    }

    /**
     * Same as {@link Builder#withToken(ValueProvider)} but without {@link ValueProvider}.
     *
     * @param token for HEC event collector
     * @return {@link Builder}
     */
    Builder withToken(String token) {
      checkArgument(token != null, "withToken(token) called with null input.");
      return setToken(ValueProvider.StaticValueProvider.of(token));
    }

    /**
     * Sets the inputBatchCount.
     *
     * @param inputBatchCount for batching post requests.
     * @return {@link Builder}
     */
    Builder withInputBatchCount(ValueProvider<Integer> inputBatchCount) {
      return setInputBatchCount(inputBatchCount);
    }

    /**
     * Method to disable certificate validation.
     *
     * @param disableCertificateValidation for disabling certificate validation.
     * @return {@link Builder}
     */
    Builder withDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation) {
      return setDisableCertificateValidation(disableCertificateValidation);
    }

    /**
     * Method to set the root CA certificate path.
     *
     * @param rootCaCertificatePath Path to root CA certificate
     * @return {@link Builder}
     */
    public Builder withRootCaCertificatePath(ValueProvider<String> rootCaCertificatePath) {
      return setRootCaCertificatePath(rootCaCertificatePath);
    }

    /**
     * Method to enable batch logs.
     *
     * @param enableBatchLogs for enabling batch logs.
     * @return {@link Builder}
     */
    public Builder withEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs) {
      return setEnableBatchLogs(enableBatchLogs);
    }

    /**
     * Method to specify if HTTP requests sent to Splunk should be GZIP encoded.
     *
     * @param enableGzipHttpCompression whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Builder withEnableGzipHttpCompression(ValueProvider<Boolean> enableGzipHttpCompression) {
      return setEnableGzipHttpCompression(enableGzipHttpCompression);
    }

    /** Builds a new {@link SplunkEventWriter} objects based on the configuration. */
    SplunkEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(token(), "token needs to be provided.");

      return autoBuild();
    }
  }
}
