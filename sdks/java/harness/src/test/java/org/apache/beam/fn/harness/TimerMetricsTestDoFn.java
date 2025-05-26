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
package org.apache.beam.fn.harness;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMetricsTestDoFn extends DoFn<KV<String, Long>, KV<String, Long>> {

  private static final Logger LOG = LoggerFactory.getLogger(TimerMetricsTestDoFn.class);

  public static final String TIMER_ID = "myTestTimer";

  @TimerId(TIMER_ID)
  @SuppressWarnings("UnusedVariable")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final Counter timersFiredCounter =
      Metrics.counter(TimerMetricsTestDoFn.class, "timersFired");

  @ProcessElement
  public void processElement(ProcessContext c, @TimerId(TIMER_ID) Timer timer) {
    LOG.info("Processing element: {}", c.element());
    // Set a timer to fire very quickly for processing time.
    timer.offset(Duration.millis(1)).setRelative();
    LOG.info("Set timer for element: {}", c.element());
    c.output(c.element());
  }

  @OnTimer(TIMER_ID)
  public void onTimerCallback(OnTimerContext c, @Key String key) {
    LOG.info("Timer fired for key: {}, window: {}, timestamp: {}", key, c.window(), c.timestamp());
    timersFiredCounter.inc();
  }
}
