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
package org.apache.beam.sdk.nexmark.queries;

import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

/** Base class for 'NEXMark' queries. */
public abstract class NexmarkQueryTransform
    extends PTransform<PCollection<Event>, PCollection<TimestampedValue<KnownSize>>> {

  final NexmarkConfiguration configuration;
  public final Monitor<Event> eventMonitor;
  public final Monitor<KnownSize> resultMonitor;
  private final Monitor<Event> endOfStreamMonitor;
  private final Counter fatalCounter;
  private transient PCollection<KV<Long, String>> sideInput = null;

  protected NexmarkQueryTransform(NexmarkConfiguration configuration, String name) {
    super(name);
    this.configuration = configuration;
    if (configuration.debug) {
      eventMonitor = new Monitor<>(name + ".Events", "event");
      resultMonitor = new Monitor<>(name + ".Results", "result");
      endOfStreamMonitor = new Monitor<>(name + ".EndOfStream", "end");
      fatalCounter = Metrics.counter(name, "fatal");
    } else {
      eventMonitor = null;
      resultMonitor = null;
      endOfStreamMonitor = null;
      fatalCounter = null;
    }
  }

  /** Implement the actual query. All we know about the result is it has a known encoded size. */
  protected abstract PCollection<KnownSize> applyPrim(PCollection<Event> events);

  /** Whether this query expects a side input to be populated. Defaults to {@code false}. */
  public boolean needsSideInput() {
    return false;
  }

  /**
   * Set the side input for the query.
   *
   * <p>Note that due to the nature of side inputs, this instance of the query is now fixed and can
   * only be safely applied in the pipeline where the side input was created.
   */
  public void setSideInput(PCollection<KV<Long, String>> sideInput) {
    this.sideInput = sideInput;
  }

  /** Get the side input, if any. */
  public @Nullable PCollection<KV<Long, String>> getSideInput() {
    return sideInput;
  }

  @Override
  public PCollection<TimestampedValue<KnownSize>> expand(PCollection<Event> events) {

    if (configuration.debug) {
      events =
          events
              // Monitor events as they go by.
              .apply(name + ".Monitor", eventMonitor.getTransform())
              // Count each type of event.
              .apply(name + ".Snoop", NexmarkUtils.snoop(name));
    }

    if (configuration.cpuDelayMs > 0) {
      // Slow down by pegging one core at 100%.
      events =
          events.apply(name + ".CpuDelay", NexmarkUtils.cpuDelay(name, configuration.cpuDelayMs));
    }

    if (configuration.diskBusyBytes > 0) {
      // Slow down by forcing bytes to durable store.
      events = events.apply(name + ".DiskBusy", NexmarkUtils.diskBusy(configuration.diskBusyBytes));
    }

    // Run the query.
    PCollection<KnownSize> queryResults = applyPrim(events);

    if (configuration.debug) {
      // Monitor results as they go by.
      queryResults = queryResults.apply(name + ".Debug", resultMonitor.getTransform());
    }

    // Timestamp the query results.
    return queryResults.apply(name + ".Stamp", NexmarkUtils.stamp(name));
  }
}
