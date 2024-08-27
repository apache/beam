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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Budget of items and bytes for fetching {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) via {@link
 * WindmillStream.GetWorkStream}. Used to control how "much" work is returned from Windmill.
 */
@Internal
@AutoValue
public abstract class GetWorkBudget {

  private static final GetWorkBudget NO_BUDGET = builder().setItems(0).setBytes(0).build();

  public static GetWorkBudget.Builder builder() {
    return new AutoValue_GetWorkBudget.Builder();
  }

  /** {@link GetWorkBudget} of 0. */
  public static GetWorkBudget noBudget() {
    return NO_BUDGET;
  }

  public static GetWorkBudget from(GetWorkRequest getWorkRequest) {
    return builder()
        .setItems(getWorkRequest.getMaxItems())
        .setBytes(getWorkRequest.getMaxBytes())
        .build();
  }

  /**
   * Applies the given bytes and items delta to the current budget, returning a new {@link
   * GetWorkBudget}. Does not drop below 0.
   */
  public GetWorkBudget apply(long itemsDelta, long bytesDelta) {
    return GetWorkBudget.builder()
        .setBytes(bytes() + bytesDelta)
        .setItems(items() + itemsDelta)
        .build();
  }

  public GetWorkBudget apply(GetWorkBudget other) {
    return apply(other.items(), other.bytes());
  }

  public GetWorkBudget subtract(GetWorkBudget other) {
    return apply(-other.items(), -other.bytes());
  }

  public GetWorkBudget subtract(long items, long bytes) {
    return apply(-items, -bytes);
  }

  /** Budget of bytes for GetWork. Does not drop below 0. */
  public abstract long bytes();

  /** Budget of items for GetWork. Does not drop below 0. */
  public abstract long items();

  public abstract GetWorkBudget.Builder toBuilder();

  @Internal
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBytes(long bytes);

    public abstract Builder setItems(long budget);

    abstract long items();

    abstract long bytes();

    abstract GetWorkBudget autoBuild();

    public final GetWorkBudget build() {
      setItems(Math.max(0, items()));
      setBytes(Math.max(0, bytes()));
      return autoBuild();
    }
  }
}
