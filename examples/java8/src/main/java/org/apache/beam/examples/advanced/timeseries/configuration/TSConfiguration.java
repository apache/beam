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
package org.apache.beam.examples.advanced.timeseries.configuration;

import com.google.auto.value.AutoValue;

import java.io.Serializable;

import org.apache.beam.sdk.annotations.Experimental;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Configuration options for dealing with time series.
 */
@SuppressWarnings("serial")
@Experimental
@AutoValue
public abstract class TSConfiguration implements Serializable {

public static final String HEARTBEAT = "HB";

/**
 * Used to determine how empty values should be populated.
 * Currently not implemented
 */
public enum FillOptions {
NONE, LAST_SEEN_WINDOW
}
public abstract FillOptions fillOption();

// The down sample period which must be set.
public abstract Duration downSampleDuration();
// Set if this is a streaming pipeline.
public abstract boolean isStreaming();
// The start time which defines when back fill starts.
public abstract Instant startTime();
// The end time of if this is Batch mode, do not set for stream mode.
public abstract Instant endTime();

abstract Builder toBuilder();

/**
* Set start time to NOW in the absence of any other data.
* Set the back fill option to create Zero or Null by default
*/
public static Builder builder() {
  return new AutoValue_TSConfiguration.Builder().setStartTime(Instant.now())
      .setFillOption(TSConfiguration.FillOptions.NONE);
}

/**
 * Builder.
 */
@AutoValue.Builder
public abstract static class Builder {
  public abstract Builder setFillOption(FillOptions fillOption);
  public abstract Builder setDownSampleDuration(Duration downSampleDuration);
  public abstract Builder setIsStreaming(boolean isStreaming);
  public abstract Builder setStartTime(Instant startTime);
  public abstract Builder setEndTime(Instant endTime);
  public abstract TSConfiguration build();
  }

}
