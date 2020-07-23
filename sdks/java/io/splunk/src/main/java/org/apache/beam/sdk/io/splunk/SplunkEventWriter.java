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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} to write {@link SplunkEvent}s to Splunk's HEC endpoint. */
@AutoValue
abstract class SplunkEventWriter extends DoFn<KV<Integer, SplunkEvent>, SplunkWriteError> {

  private static final Integer DEFAULT_BATCH_COUNT = 1;
  private static final Boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  private static final Logger LOG = LoggerFactory.getLogger(SplunkEventWriter.class);
  private static final long DEFAULT_FLUSH_DELAY = 2;
  private static final Counter INPUT_COUNTER =
      Metrics.counter(SplunkEventWriter.class, "inbound-events");
  private static final Counter SUCCESS_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-successful-events");
  private static final Counter FAILED_WRITES =
      Metrics.counter(SplunkEventWriter.class, "outbound-failed-events");
  private static final String BUFFER_STATE_NAME = "buffer";
  private static final String COUNT_STATE_NAME = "count";
  private static final String TIME_ID_NAME = "expiry";

  @StateId(BUFFER_STATE_NAME)
  private final StateSpec<BagState<SplunkEvent>> buffer = StateSpecs.bag();

  @StateId(COUNT_STATE_NAME)
  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

  @TimerId(TIME_ID_NAME)
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private Integer batchCount;
  private Boolean disableValidation;
  private HttpEventPublisher publisher;

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

  @Setup
  public void setup() {

    checkArgument(url().isAccessible(), "url is required for writing events.");
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

    try {
      HttpEventPublisher.Builder builder =
          HttpEventPublisher.newBuilder()
              .withUrl(url().get())
              .withToken(token().get())
              .withDisableCertificateValidation(disableValidation);

      publisher = builder.build();
      LOG.info("Successfully created HttpEventPublisher");

    } catch (NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | UnsupportedEncodingException e) {
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

      LOG.info("Flushing batch of {} events", count);
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
      LOG.info("Flushing window with {} events", countState.read());
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
      try {
        // Important to close this response to avoid connection leak.
        response = publisher.execute(events);

        if (!response.isSuccessStatusCode()) {
          flushWriteFailures(
              events, response.getStatusMessage(), response.getStatusCode(), receiver);
          logWriteFailures(countState);

        } else {
          LOG.info("Successfully wrote {} events", countState.read());
          SUCCESS_WRITES.inc(countState.read());
        }

      } catch (HttpResponseException e) {
        LOG.error(
            "Error writing to Splunk. StatusCode: {}, content: {}, StatusMessage: {}",
            e.getStatusCode(),
            e.getContent(),
            e.getStatusMessage());
        logWriteFailures(countState);

        flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

      } catch (IOException ioe) {
        LOG.error("Error writing to Splunk: {}", ioe.getMessage());
        logWriteFailures(countState);

        flushWriteFailures(events, ioe.getMessage(), null, receiver);

      } finally {
        // States are cleared regardless of write success or failure since we
        // write failed events to an output PCollection.
        bufferState.clear();
        countState.clear();

        if (response != null) {
          response.disconnect();
        }
      }
    }
  }

  /** Logs write failures and update {@link Counter}. */
  private void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
    LOG.error("Failed to write {} events", countState.read());
    FAILED_WRITES.inc(countState.read());
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

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setUrl(ValueProvider<String> url);

    abstract ValueProvider<String> url();

    abstract Builder setToken(ValueProvider<String> token);

    abstract ValueProvider<String> token();

    abstract Builder setDisableCertificateValidation(
        ValueProvider<Boolean> disableCertificateValidation);

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

    /** Builds a new {@link SplunkEventWriter} objects based on the configuration. */
    SplunkEventWriter build() {
      checkNotNull(url(), "url needs to be provided.");
      checkNotNull(token(), "token needs to be provided.");

      return autoBuild();
    }
  }
}
