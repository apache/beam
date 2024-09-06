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
import org.apache.beam.sdk.annotations.Internal;

/** Keep track of any operational limits required by the backend. */
@AutoValue
@Internal
public abstract class OperationalLimits {

  private static final long DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES = 180 << 20;

  // Maximum size of a commit from a single work item.
  public abstract long getMaxWorkItemCommitBytes();
  // Maximum size of a single output element's serialized key.
  public abstract long getMaxOutputKeyBytes();
  // Maximum size of a single output element's serialized value.
  public abstract long getMaxOutputValueBytes();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMaxWorkItemCommitBytes(long bytes);

    public abstract Builder setMaxOutputKeyBytes(long bytes);

    public abstract Builder setMaxOutputValueBytes(long bytes);

    public abstract OperationalLimits build();
  }

  public static OperationalLimits.Builder builder() {
    return new AutoValue_OperationalLimits.Builder()
        .setMaxWorkItemCommitBytes(DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES)
        .setMaxOutputKeyBytes(Long.MAX_VALUE)
        .setMaxOutputValueBytes(Long.MAX_VALUE);
  }
}
