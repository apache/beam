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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoValue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@Internal
public abstract class MultiKeyBundleOptions {
  // TODO: consider moving this to be part of PipelineOptions after the feature is stable

  private static final Logger LOG = LoggerFactory.getLogger(MultiKeyBundleOptions.class);

  // Don't use. Experiment guarding multi key bundles. The feature is work in progress and
  // incomplete.
  public static final String UNSTABLE_ENABLE_MULTI_KEY_BUNDLE = "unstable_enable_multi_key_bundle";

  private static final String WINDMILL_MAX_KEY_GROUP_BATCH_SIZE =
      "windmill_max_key_group_batch_size";
  private static final String WINDMILL_MAX_KEY_GROUP_BATCH_TIME_MS =
      "windmill_max_key_group_batch_time_ms";
  private static final String WINDMILL_MAX_KEY_GROUP_BATCH_SINK_BYTES =
      "windmill_max_key_group_batch_sink_bytes";

  public abstract int maxKeyGroupBatchSize();

  public abstract long maxKeyGroupBatchTimeNanos();

  public abstract boolean multiKeyBundleEnabled();

  public abstract long maxKeyGroupBatchSinkBytes();

  public static Builder builder() {
    return new AutoValue_MultiKeyBundleOptions.Builder();
  }

  public static MultiKeyBundleOptions fromOptions(PipelineOptions options) {
    int maxKeyGroupBatchSize =
        tryParseInt(
            ExperimentalOptions.getExperimentValue(options, WINDMILL_MAX_KEY_GROUP_BATCH_SIZE),
            100,
            WINDMILL_MAX_KEY_GROUP_BATCH_SIZE);

    long batchTimeMs =
        tryParseLong(
            ExperimentalOptions.getExperimentValue(options, WINDMILL_MAX_KEY_GROUP_BATCH_TIME_MS),
            100,
            WINDMILL_MAX_KEY_GROUP_BATCH_TIME_MS);

    boolean multiKeyBundleEnabled =
        ExperimentalOptions.hasExperiment(options, UNSTABLE_ENABLE_MULTI_KEY_BUNDLE);

    long maxKeyGroupBatchSinkBytes =
        tryParseLong(
            ExperimentalOptions.getExperimentValue(
                options, WINDMILL_MAX_KEY_GROUP_BATCH_SINK_BYTES),
            StreamingDataflowWorker.MAX_SINK_BYTES,
            WINDMILL_MAX_KEY_GROUP_BATCH_SINK_BYTES);

    return builder()
        .setMaxKeyGroupBatchSize(maxKeyGroupBatchSize)
        .setMaxKeyGroupBatchTimeNanos(TimeUnit.MILLISECONDS.toNanos(batchTimeMs))
        .setMultiKeyBundleEnabled(multiKeyBundleEnabled)
        .setMaxKeyGroupBatchSinkBytes(maxKeyGroupBatchSinkBytes)
        .build();
  }

  private static int tryParseInt(@Nullable String value, int defaultValue, String experimentName) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      LOG.warn(
          "Failed to parse experiment {} value '{}' as integer, falling back to default: {}",
          experimentName,
          value,
          defaultValue,
          e);
      return defaultValue;
    }
  }

  private static long tryParseLong(
      @Nullable String value, long defaultValue, String experimentName) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      LOG.warn(
          "Failed to parse experiment {} value '{}' as long, falling back to default: {}",
          experimentName,
          value,
          defaultValue,
          e);
      return defaultValue;
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMaxKeyGroupBatchSize(int size);

    public abstract Builder setMaxKeyGroupBatchTimeNanos(long nanos);

    public abstract Builder setMultiKeyBundleEnabled(boolean enabled);

    public abstract Builder setMaxKeyGroupBatchSinkBytes(long bytes);

    public abstract MultiKeyBundleOptions build();
  }
}
