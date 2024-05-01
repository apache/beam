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
package org.apache.beam.runners.dataflow.worker.windmill.work;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Context to process {@link WorkItem} */
@AutoValue
@Internal
public abstract class WorkProcessingContext {
  public static WorkProcessingContext.Builder builder() {
    return new AutoValue_WorkProcessingContext.Builder();
  }

  public static WorkProcessingContext.Builder builder(
      String computationId,
      BiFunction<String, KeyedGetDataRequest, Optional<KeyedGetDataResponse>> keyedDataFetcher) {
    return builder()
        .setComputationId(computationId)
        .setKeyedDataFetcher(request -> keyedDataFetcher.apply(computationId, request));
  }

  public static WorkProcessingContext.Builder builder(
      String computationId, GetDataStream getDataStream) {
    return builder()
        .setComputationId(computationId)
        .setGetDataStream(getDataStream)
        .setKeyedDataFetcher(
            request -> Optional.ofNullable(getDataStream.requestKeyedData(computationId, request)));
  }

  public abstract String computationId();

  public abstract Instant inputDataWatermark();

  public abstract @Nullable Instant synchronizedProcessingTime();

  public abstract @Nullable Instant outputDataWatermark();

  public abstract @Nullable GetDataStream getDataStream();

  /** {@link WorkItem} being processed. */
  public abstract WorkItem workItem();

  /**
   * {@link GetDataStream} that connects to the backend Windmill worker handling the {@link
   * WorkItem}.
   */
  public abstract Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>> keyedDataFetcher();

  /**
   * {@link WorkCommitter} that commits completed work to the backend Windmill worker handling the
   * {@link WorkItem}.
   */
  public abstract Consumer<Commit> workCommitter();

  public final void queueCommit(Commit commit) {
    workCommitter().accept(commit);
  }

  @Memoized
  public ShardedKey shardedKey() {
    return ShardedKey.create(workItem().getKey(), workItem().getShardingKey());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setComputationId(String value);

    public abstract Builder setInputDataWatermark(Instant value);

    public abstract Builder setSynchronizedProcessingTime(@Nullable Instant value);

    public abstract Builder setOutputDataWatermark(@Nullable Instant value);

    public abstract Builder setWorkItem(WorkItem value);

    public abstract Builder setKeyedDataFetcher(
        Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>> value);

    public abstract Builder setWorkCommitter(Consumer<Commit> value);

    public abstract Builder setGetDataStream(GetDataStream value);

    abstract WorkProcessingContext autoBuild();

    public final WorkProcessingContext build() {
      WorkProcessingContext workProcessingContext = autoBuild();
      // May be null if output watermark not yet known.
      Preconditions.checkState(
          workProcessingContext.outputDataWatermark() == null
              || !workProcessingContext
                  .outputDataWatermark()
                  .isAfter(workProcessingContext.inputDataWatermark()));
      return workProcessingContext;
    }
  }
}
