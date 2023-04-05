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
package org.apache.beam.sdk.nexmark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testutils.TestResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Summary of performance for a particular run of a configuration. */
public class NexmarkPerf implements TestResult {

  /**
   * A sample of the number of events and number of results (if known) generated at a particular
   * time.
   */
  public static class ProgressSnapshot {
    /** Seconds since job was started (in wallclock time). */
    @JsonProperty double secSinceStart;

    /** Job runtime in seconds (time from first event to last generated event or output result). */
    @JsonProperty double runtimeSec;

    /** Cumulative number of events generated. -1 if not known. */
    @JsonProperty long numEvents;

    /** Cumulative number of results emitted. -1 if not known. */
    @JsonProperty long numResults;

    /**
     * Return true if there looks to be activity between {@code this} and {@code that} snapshots.
     */
    public boolean anyActivity(ProgressSnapshot that) {
      if (runtimeSec != that.runtimeSec) {
        // An event or result end timestamp looks to have changed.
        return true;
      }
      if (numEvents != that.numEvents) {
        // Some more events were generated.
        return true;
      }
      if (numResults != that.numResults) {
        // Some more results were emitted.
        return true;
      }
      return false;
    }
  }

  /** Progess snapshots. Null if not yet calculated. */
  @JsonProperty public @Nullable List<ProgressSnapshot> snapshots = null;

  /**
   * Effective runtime, in seconds. Measured from timestamp of first generated event to latest of
   * timestamp of last generated event and last emitted result. -1 if not known.
   */
  @JsonProperty public double runtimeSec = -1.0;

  /** Number of events generated. -1 if not known. */
  @JsonProperty public long numEvents = -1;

  /**
   * Number of events generated per second of runtime. For batch this is number of events over the
   * above runtime. For streaming this is the 'steady-state' event generation rate sampled over the
   * lifetime of the job. -1 if not known.
   */
  @JsonProperty public double eventsPerSec = -1.0;

  /** Number of event bytes generated per second of runtime. -1 if not known. */
  @JsonProperty public double eventBytesPerSec = -1.0;

  /** Number of results emitted. -1 if not known. */
  @JsonProperty public long numResults = -1;

  /** Number of results generated per second of runtime. -1 if not known. */
  @JsonProperty public double resultsPerSec = -1.0;

  /** Number of result bytes generated per second of runtime. -1 if not known. */
  @JsonProperty public double resultBytesPerSec = -1.0;

  /** Delay between start of job and first event in second. -1 if not known. */
  @JsonProperty public double startupDelaySec = -1.0;

  /** Delay between first event and first result in seconds. -1 if not known. */
  @JsonProperty public double processingDelaySec = -1.0;

  /** Delay between last result and job completion in seconds. -1 if not known. */
  @JsonProperty public double shutdownDelaySec = -1.0;

  /**
   * Time-dilation factor. Calculate as event time advancement rate relative to real time. Greater
   * than one implies we processed events faster than they would have been generated in real time.
   * Less than one implies we could not keep up with events in real time. -1 if not known.
   */
  @JsonProperty double timeDilation = -1.0;

  /** List of errors encountered during job execution. */
  @JsonProperty public @Nullable List<String> errors = null;

  /** The job id this perf was drawn from. Null if not known. */
  @JsonProperty public @Nullable String jobId = null;

  /** Return a JSON representation of performance. */
  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Parse a {@link NexmarkPerf} object from JSON {@code string}. */
  public static NexmarkPerf fromString(String string) {
    try {
      return NexmarkUtils.MAPPER.readValue(string, NexmarkPerf.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse nexmark perf: ", e);
    }
  }

  /**
   * Return true if there looks to be activity between {@code this} and {@code that} perf values.
   */
  public boolean anyActivity(NexmarkPerf that) {
    if (runtimeSec != that.runtimeSec) {
      // An event or result end timestamp looks to have changed.
      return true;
    }
    if (numEvents != that.numEvents) {
      // Some more events were generated.
      return true;
    }
    if (numResults != that.numResults) {
      // Some more results were emitted.
      return true;
    }
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    return NexmarkUtils.MAPPER.convertValue(this, new TypeReference<Map<String, Object>>() {});
  }
}
