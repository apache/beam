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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Budget of items and bytes for fetching {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) via {@link
 * WindmillStream.GetWorkStream}. Used to control how "much" work is returned from Windmill.
 */
@AutoValue
public abstract class GetWorkBudget {
  public static GetWorkBudget.Builder builder() {
    return new AutoValue_GetWorkBudget.Builder();
  }

  /** {@link GetWorkBudget} of 0. */
  public static GetWorkBudget noBudget() {
    return builder().setItems(0).setBytes(0).build();
  }

  public static GetWorkBudget from(GetWorkRequest getWorkRequest) {
    return builder()
        .setItems(getWorkRequest.getMaxItems())
        .setBytes(getWorkRequest.getMaxBytes())
        .build();
  }

  /**
   * Adds the given bytes and items or the current budget, returning a new {@link GetWorkBudget}.
   * Does not drop below 0.
   */
  public GetWorkBudget add(long items, long bytes) {
    Preconditions.checkArgument(items >= 0 && bytes >= 0);
    return GetWorkBudget.builder().setBytes(bytes() + bytes).setItems(items() + items).build();
  }

  public GetWorkBudget add(GetWorkBudget other) {
    return add(other.items(), other.bytes());
  }

  /**
   * Subtracts the given bytes and items or the current budget, returning a new {@link
   * GetWorkBudget}. Does not drop below 0.
   */
  public GetWorkBudget subtract(long items, long bytes) {
    Preconditions.checkArgument(items >= 0 && bytes >= 0);
    return GetWorkBudget.builder().setBytes(bytes() - bytes).setItems(items() - items).build();
  }

  public GetWorkBudget subtract(GetWorkBudget other) {
    return subtract(other.items(), other.bytes());
  }

  /** Budget of bytes for GetWork. Does not drop below 0. */
  public abstract long bytes();

  /** Budget of items for GetWork. Does not drop below 0. */
  public abstract long items();

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
