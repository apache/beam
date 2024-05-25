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
package org.apache.beam.runners.dataflow.worker.streaming;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;

/** Watermarks for stream pipeline processing. */
@AutoValue
@Internal
public abstract class Watermarks {

  public static Builder builder() {
    return new AutoValue_Watermarks.Builder();
  }

  public abstract Instant inputDataWatermark();

  public abstract @Nullable Instant synchronizedProcessingTime();

  public abstract @Nullable Instant outputDataWatermark();

  @AutoValue.Builder
  public abstract static class Builder {
    private static boolean hasValidOutputDataWatermark(Watermarks watermarks) {
      @Nullable Instant outputDataWatermark = watermarks.outputDataWatermark();
      return outputDataWatermark == null
          || !outputDataWatermark.isAfter(watermarks.inputDataWatermark());
    }

    public abstract Builder setInputDataWatermark(Instant value);

    public abstract Builder setSynchronizedProcessingTime(@Nullable Instant value);

    public abstract Builder setOutputDataWatermark(@Nullable Instant value);

    public final Builder setOutputDataWatermark(long outputDataWatermark) {
      return setOutputDataWatermark(
          WindmillTimeUtils.windmillToHarnessWatermark(outputDataWatermark));
    }

    abstract Watermarks autoBuild();

    public final Watermarks build() {
      Watermarks watermarks = autoBuild();
      Preconditions.checkState(hasValidOutputDataWatermark(watermarks));
      return watermarks;
    }
  }
}
