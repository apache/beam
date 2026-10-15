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
package org.apache.beam.sdk.io.components.throttling;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around the AdaptiveThrottler that also handles logging and signaling throttling to the
 * SDK harness using the provided namespace.
 *
 * <p>For usage, instantiate one instance of a ReactiveThrottler class for a PTransform. When making
 * remote calls to a service, preface that call with the throttle() method to potentially
 * pre-emptively throttle the request. This will throttle future calls based on the failure rate of
 * preceding calls, with higher failure rates making it more likely that a call is delayed, to allow
 * system recovery. A throttled call is delayed once by throttleDelaySecs and then proceeds; it is
 * never dropped. Call throttle() exactly once per attempt (including retries), since each call is
 * recorded as one request attempt. Then capture the timestamp of the attempted request and execute
 * the request code. On a success, call successfulRequest(timestamp) to report the success to the
 * throttler; attempts that are not reported as successful count as failures.
 */
@SuppressFBWarnings(
    value = "CT_CONSTRUCTOR_THROW",
    justification =
        "Pre-existing finding, not triaged yet."
            + " Making the class final or moving the throwing code"
            + " into a static factory method may fix it.")
public class ReactiveThrottler extends AdaptiveThrottler {
  private static final Logger LOG = LoggerFactory.getLogger(ReactiveThrottler.class);
  private static final long SECONDS_TO_MILLISECONDS = 1000L;

  private final ThrottlingSignaler throttlingSignaler;
  private final int throttleDelaySecs;

  /**
   * Initializes the ReactiveThrottler.
   *
   * @param samplePeriodMs length of history to consider, in ms, to set throttling.
   * @param sampleUpdateMs granularity of time buckets that we store data in, in ms.
   * @param overloadRatio the target ratio between requests sent and successful requests.
   * @param namespace the namespace to use for logging and signaling throttling is occurring.
   * @param throttleDelaySecs the amount of time in seconds to wait after preemptively throttled
   *     requests.
   */
  public ReactiveThrottler(
      long samplePeriodMs,
      long sampleUpdateMs,
      double overloadRatio,
      String namespace,
      int throttleDelaySecs) {
    super(samplePeriodMs, sampleUpdateMs, overloadRatio);
    if (throttleDelaySecs <= 0) {
      throw new IllegalArgumentException("throttleDelaySecs must be greater than 0");
    }
    this.throttlingSignaler = new ThrottlingSignaler(namespace);
    this.throttleDelaySecs = throttleDelaySecs;
  }

  /**
   * Delays the request by throttleDelaySecs if the underlying AdaptiveThrottler elects to
   * preemptively throttle it, based on the failure rate of recent requests. Automatically handles
   * logging the throttling and signaling to the SDK harness that the request is being throttled.
   *
   * <p>This should be called once for each attempt to contact a remote service (including
   * retries), prior to the call being performed. Each call is recorded as one request attempt, and
   * delays the request at most once.
   */
  public void throttle() throws InterruptedException {
    if (throttleRequest(System.currentTimeMillis())) {
      LOG.debug("Delaying request for {} seconds due to previous failures", throttleDelaySecs);
      Thread.sleep(throttleDelaySecs * SECONDS_TO_MILLISECONDS);
      throttlingSignaler.signalThrottling(throttleDelaySecs * SECONDS_TO_MILLISECONDS);
    }
  }
}
