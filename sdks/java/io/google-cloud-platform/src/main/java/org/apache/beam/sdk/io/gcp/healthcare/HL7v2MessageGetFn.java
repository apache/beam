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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.io.IOException;
import java.text.ParseException;
import org.apache.beam.sdk.io.gcp.datastore.AdaptiveThrottler;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn for fetching messages from the HL7v2 store with error handling. */
class HL7v2MessageGetFn extends DoFn<String, FailsafeElement<String, Message>> {

  private Counter failedMessageReads = Metrics.counter(HL7v2MessageGetFn.class, "failed-message-reads");
  private static final Logger LOG = LoggerFactory.getLogger(HL7v2MessageGetFn.class);
  private final Counter throttledSeconds =
      Metrics.counter(HL7v2MessageGetFn.class, "cumulativeThrottlingSeconds");
  private final Counter successfulHL7v2MessageGets =
      Metrics.counter(HL7v2MessageGetFn.class, "successfulHL7v2MessageGets");
  private HealthcareApiClient client;
  private transient AdaptiveThrottler throttler;

  /** Instantiates a new Hl 7 v 2 message get fn. */
  HL7v2MessageGetFn() {}

  /**
   * Instantiate healthcare client.
   *
   * @throws IOException the io exception
   */
  @Setup
  public void instantiateHealthcareClient() throws IOException {
    this.client = new HttpHealthcareApiClient();
  }

  /**
   * Start bundle.
   *
   * @param context the context.
   */
  @StartBundle
  public void startBundle(StartBundleContext context) {
    if (throttler == null) {
      throttler = new AdaptiveThrottler(1200000, 10000, 1.25);
    }
  }

  /**
   * Process element.
   *
   * @param context the context
   */
  @ProcessElement
  public void processElement(ProcessContext context) {
    String msgId = context.element();
    try {
      context.output(FailsafeElement.of(msgId, fetchMessage(this.client, msgId)));
    } catch (Exception e) {
      failedMessageReads.inc();
      LOG.warn(
          String.format(
              "Error fetching HL7v2 message with ID %s writing to Dead Letter "
                  + "Queue. Cause: %s Stack Trace: %s",
              msgId, e.getMessage(), Throwables.getStackTraceAsString(e)));
      context.output(
          FetchHL7v2Message.DEAD_LETTER,
          FailsafeElement.of(msgId, new Message())
              .setErrorMessage(e.getMessage())
              .setStacktrace(Throwables.getStackTraceAsString(e)));
    }
  }

  private Message fetchMessage(HealthcareApiClient client, String msgId)
      throws IOException, ParseException, IllegalArgumentException, InterruptedException {

    long startTime = System.currentTimeMillis();
    Sleeper sleeper = Sleeper.DEFAULT;
    if (throttler.throttleRequest(startTime)) {
      LOG.info(String.format("Delaying request for %s due to previous failures.", msgId));
      this.throttledSeconds.inc(5); // TODO avoid magic numbers.
      sleeper.sleep(5000);
    }

    Message msg = client.getHL7v2Message(msgId);
    this.throttler.successfulRequest(startTime);
    this.successfulHL7v2MessageGets.inc();
    if (msg == null) {
      throw new IOException(String.format("GET request for %s returned null", msgId));
    }
    return msg;
  }
}
