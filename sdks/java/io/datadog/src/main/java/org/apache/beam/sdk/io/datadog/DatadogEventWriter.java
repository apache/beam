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

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} to write {@link DatadogEvent}s to Datadog's Logs API. */
@AutoValue
public abstract class DatadogEventWriter
    extends DoFn<KV<Integer, DatadogEvent>, DatadogWriteError> {

  private static final Integer MIN_BATCH_COUNT = 10;
  private static final Integer DEFAULT_BATCH_COUNT = 100;
  private static final Integer MAX_BATCH_COUNT = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(DatadogEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Long MAX_BUFFER_SIZE = 5L * 1000 * 1000; // 5MB
  private static final Counter INPUT_COUNTER =
      Metrics.counter(DatadogEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(DatadogEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(DatadogEventWriter.class, "outbound-failed-events");
  private static final Counter INVALID_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-invalid-requests");
  private static final Counter SERVER_ERROR_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-server-error-requests");
  private static final Counter VALID_REQUESTS =
      Metrics.counter(DatadogEventWriter.class, "http-valid-requests");
  private static final Distribution SUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(DatadogEventWriter.class, "successful_write_to_datadog_latency_ms");
  private static final Distribution UNSUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(DatadogEventWriter.class, "unsuccessful_write_to_datadog_latency_ms");
  private static final Distribution SUCCESSFUL_WRITE_BATCH_SIZE =
      Metrics.distribution(DatadogEventWriter.class, "write_to_datadog_batch");
  private static final Distribution SUCCESSFUL_WRITE_PAYLOAD_SIZE =
      Metrics.distribution(DatadogEventWriter.class, "write_to_datadog_bytes");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String BUFFER_SIZE_STATE_NAME = "buffer_size";
  private static final String TIME_ID_NAME = "expiry";
  private static final Pattern URL_PATTERN = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");

  @VisibleForTesting
  protected static final String INVALID_URL_FORMAT_MESSAGE =
      "Invalid url format. Url format should match PROTOCOL://HOST[:PORT], where PORT is optional. "
          + "Supported Protocols are http and https. eg: http://hostname:8088";

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<DatadogEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @StateId(BUFFER_SIZE_STATE_NAME)
  private final StateSpec<ValueState<Long>> bufferSize = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  private Integer batchCount;
  private Long maxBufferSize;
  @Nullable private transient DatadogEventPublisher publisher;

  DatadogEventWriter() {
    this.batchCount = DEFAULT_BATCH_COUNT;
    this.maxBufferSize = MAX_BUFFER_SIZE;
    this.publisher = null;
  }

  public static Builder newBuilder() {
    return newBuilder(MIN_BATCH_COUNT);
  }

  public static Builder newBuilder(@Nullable Integer minBatchCount) {
    return new AutoValue_DatadogEventWriter.Builder()
        .setMinBatchCount(MoreObjects.firstNonNull(minBatchCount, MIN_BATCH_COUNT));
  }

  @Nullable
  abstract String url();

  @Nullable
  abstract String apiKey();

  @Nullable
  abstract Integer minBatchCount();

  @Nullable
  abstract Integer inputBatchCount();

  @Nullable
  abstract Long maxBufferSize();

  @Setup
  public void setup() {

    final String url = url();
    if (url == null) {
      throw new IllegalArgumentException("url is required for writing events.");
    }
    checkArgument(isValidUrlFormat(url), INVALID_URL_FORMAT_MESSAGE);
    final String apiKey = apiKey();
    if (apiKey == null) {
      throw new IllegalArgumentException("API Key is required for writing events.");
    }

    batchCount = MoreObjects.firstNonNull(inputBatchCount(), DEFAULT_BATCH_COUNT);
    LOG.info("Batch count set to: {}", batchCount);

    maxBufferSize = MoreObjects.firstNonNull(maxBufferSize(), MAX_BUFFER_SIZE);
    LOG.info("Max buffer size set to: {}", maxBufferSize);

    checkArgument(
        batchCount >= MoreObjects.firstNonNull(minBatchCount(), MIN_BATCH_COUNT),
        "batchCount must be greater than or equal to %s",
        minBatchCount());
    checkArgument(
        batchCount <= MAX_BATCH_COUNT,
        "batchCount must be less than or equal to %s",
        MAX_BATCH_COUNT);

    try {
      DatadogEventPublisher.Builder builder =
          DatadogEventPublisher.newBuilder().withUrl(url).withApiKey(apiKey);

      publisher = builder.build();
    } catch (IOException | NoSuchAlgorithmException | KeyManagementException e) {
      LOG.error("Error creating HttpEventPublisher: ", e);
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, DatadogEvent> input,
      OutputReceiver<DatadogWriteError> receiver,
      BoundedWindow window,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @StateId(BUFFER_SIZE_STATE_NAME) ValueState<Long> bufferSizeState,
      @TimerId(TIME_ID_NAME) Timer timer)
      throws IOException {

    DatadogEvent event = input.getValue();
    INPUT_COUNTER.inc();

    String eventPayload = DatadogEventSerializer.getPayloadString(event);
    long eventPayloadSize = DatadogEventSerializer.getPayloadSize(eventPayload);
    if (eventPayloadSize > maxBufferSize) {
      LOG.error(
          "Error processing event of size {} due to exceeding max buffer size", eventPayloadSize);
      DatadogWriteError error = DatadogWriteError.newBuilder().withPayload(eventPayload).build();
      receiver.output(error);
      return;
    }

    timer.offset(Duration.standardSeconds(DEFAULT_FLUSH_DELAY)).setRelative();

    long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    long bufferSize = MoreObjects.<Long>firstNonNull(bufferSizeState.read(), 0L);
    if (bufferSize + eventPayloadSize > maxBufferSize) {
      LOG.debug("Flushing batch of {} events of size {} due to max buffer size", count, bufferSize);
      flush(receiver, bufferState, countState, bufferSizeState);

      count = 0L;
      bufferSize = 0L;
    }

    bufferState.add(event);

    count = count + 1L;
    countState.write(count);

    bufferSize = bufferSize + eventPayloadSize;
    bufferSizeState.write(bufferSize);

    if (count >= batchCount) {
      LOG.debug("Flushing batch of {} events of size {} due to batch count", count, bufferSize);
      flush(receiver, bufferState, countState, bufferSizeState);
    }
  }

  @OnTimer(TIME_ID_NAME)
  public void onExpiry(
      OutputReceiver<DatadogWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @StateId(BUFFER_SIZE_STATE_NAME) ValueState<Long> bufferSizeState)
      throws IOException {

    long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
    long bufferSize = MoreObjects.<Long>firstNonNull(bufferSizeState.read(), 0L);

    if (count > 0) {
      LOG.debug("Flushing batch of {} events of size {} due to timer", count, bufferSize);
      flush(receiver, bufferState, countState, bufferSizeState);
    }
  }

  @Teardown
  public void tearDown() {
    if (this.publisher != null) {
      try {
        this.publisher.close();
        LOG.info("Successfully closed HttpEventPublisher");

      } catch (IOException e) {
        LOG.warn("Received exception while closing HttpEventPublisher: ", e);
      }
    }
  }

  /**
   * Utility method to flush a batch of events via {@link DatadogEventPublisher}.
   *
   * @param receiver Receiver to write {@link DatadogWriteError}s to
   */
  private void flush(
      OutputReceiver<DatadogWriteError> receiver,
      @StateId(BUFFER_STATE_NAME) BagState<DatadogEvent> bufferState,
      @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
      @StateId(BUFFER_SIZE_STATE_NAME) ValueState<Long> bufferSizeState)
      throws IOException {

    if (!bufferState.isEmpty().read()) {

      long count = MoreObjects.firstNonNull(countState.read(), 0L);
      long bufferSize = MoreObjects.firstNonNull(bufferSizeState.read(), 0L);
      HttpResponse response = null;
      List<DatadogEvent> events = Lists.newArrayList(bufferState.read());
      long startTime = System.nanoTime();
      try {
        // Important to close this response to avoid connection leak.
        response = checkNotNull(publisher).execute(events);
        if (!response.isSuccessStatusCode()) {
          UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          FAILED_WRITES.inc(count);
          int statusCode = response.getStatusCode();
          if (statusCode >= 400 && statusCode < 500) {
            INVALID_REQUESTS.inc();
          } else if (statusCode >= 500 && statusCode < 600) {
            SERVER_ERROR_REQUESTS.inc();
          }

          logWriteFailures(
              count,
              response.getStatusCode(),
              response.parseAsString(),
              response.getStatusMessage());
          flushWriteFailures(
              events, response.getStatusMessage(), response.getStatusCode(), receiver);

        } else {
          SUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
          SUCCESS_WRITES.inc(count);
          VALID_REQUESTS.inc();
          SUCCESSFUL_WRITE_BATCH_SIZE.update(count);
          SUCCESSFUL_WRITE_PAYLOAD_SIZE.update(bufferSize);

          LOG.debug("Successfully wrote {} events", count);
        }

      } catch (HttpResponseException e) {
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(count);
        int statusCode = e.getStatusCode();
        if (statusCode >= 400 && statusCode < 500) {
          INVALID_REQUESTS.inc();
        } else if (statusCode >= 500 && statusCode < 600) {
          SERVER_ERROR_REQUESTS.inc();
        }

        logWriteFailures(count, e.getStatusCode(), e.getContent(), e.getStatusMessage());
        flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

      } catch (IOException ioe) {
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(nanosToMillis(System.nanoTime() - startTime));
        FAILED_WRITES.inc(count);
        INVALID_REQUESTS.inc();

        logWriteFailures(count, 0, ioe.getMessage(), null);
        flushWriteFailures(events, ioe.getMessage(), null, receiver);

      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed events to an output PCollection.
        bufferState.clear();
        countState.clear();
        bufferSizeState.clear();

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
              "Error ignoring response from Datadog. Messages should still have published, but there"
                  + " might be a connection leak.",
              e);
        }
      }
    }
  }

  /** Utility method to log write failures. */
  private void logWriteFailures(
      long count, int statusCode, @Nullable String content, @Nullable String statusMessage) {
    LOG.error("Failed to write {} events", count);
    LOG.error(
        "Error writing to Datadog. StatusCode: {}, content: {}, StatusMessage: {}",
        statusCode,
        content,
        statusMessage);
  }

  /**
   * Utility method to un-batch and flush failed write events.
   *
   * @param events List of {@link DatadogEvent}s to un-batch
   * @param statusMessage Status message to be added to {@link DatadogWriteError}
   * @param statusCode Status code to be added to {@link DatadogWriteError}
   * @param receiver Receiver to write {@link DatadogWriteError}s to
   */
  private void flushWriteFailures(
      List<DatadogEvent> events,
      @Nullable String statusMessage,
      @Nullable Integer statusCode,
      OutputReceiver<DatadogWriteError> receiver) {

    checkNotNull(events, "DatadogEvents cannot be null.");

    DatadogWriteError.Builder builder = DatadogWriteError.newBuilder();

    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }

    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }

    for (DatadogEvent event : events) {
      String payload = DatadogEventSerializer.getPayloadString(event);
      DatadogWriteError error = builder.withPayload(payload).build();
      receiver.output(error);
    }
  }

  /**
   * Checks whether the Logs API URL matches the format PROTOCOL://HOST[:PORT].
   *
   * @param url for Logs API
   * @return true if the URL is valid
   */
  private static boolean isValidUrlFormat(@Nullable String url) {
    if (url == null) {
      return false;
    }
    Matcher matcher = URL_PATTERN.matcher(url);
    if (matcher.find()) {
      String host = matcher.group(2);
      if (host == null) {
        return false;
      }
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

    abstract Builder setUrl(String url);

    abstract String url();

    abstract Builder setApiKey(String apiKey);

    abstract String apiKey();

    abstract Builder setMinBatchCount(Integer minBatchCount);

    abstract Integer minBatchCount();

    abstract Builder setInputBatchCount(@Nullable Integer inputBatchCount);

    abstract Builder setMaxBufferSize(Long maxBufferSize);

    abstract DatadogEventWriter autoBuild();

    /**
     * Method to set the url for Logs API.
     *
     * @param url for Logs API
     * @return {@link Builder}
     */
    public Builder withUrl(String url) {
      checkArgument(url != null, "withURL(url) called with null input.");
      checkArgument(isValidUrlFormat(url), INVALID_URL_FORMAT_MESSAGE);
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
     * Method to set the inputBatchCount.
     *
     * @param inputBatchCount for batching post requests.
     * @return {@link Builder}
     */
    public Builder withInputBatchCount(@Nullable Integer inputBatchCount) {
      if (inputBatchCount != null) {
        checkArgument(
            inputBatchCount >= MoreObjects.firstNonNull(minBatchCount(), MIN_BATCH_COUNT),
            "inputBatchCount must be greater than or equal to %s",
            minBatchCount());
        checkArgument(
            inputBatchCount <= MAX_BATCH_COUNT,
            "inputBatchCount must be less than or equal to %s",
            MAX_BATCH_COUNT);
      }
      return setInputBatchCount(inputBatchCount);
    }

    /**
     * Method to set the maxBufferSize.
     *
     * @param maxBufferSize for batching post requests.
     * @return {@link Builder}
     */
    public Builder withMaxBufferSize(@Nullable Long maxBufferSize) {
      if (maxBufferSize == null) {
        return setMaxBufferSize(MAX_BUFFER_SIZE);
      }
      return setMaxBufferSize(maxBufferSize);
    }

    /** Build a new {@link DatadogEventWriter} objects based on the configuration. */
    public DatadogEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(apiKey(), "apiKey needs to be provided.");

      return autoBuild();
    }
  }
}
