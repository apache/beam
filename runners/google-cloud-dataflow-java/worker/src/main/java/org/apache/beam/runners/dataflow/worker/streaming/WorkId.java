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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;

/**
 * A composite key used to identify a unit of {@link Work}. If multiple units of {@link Work} have
 * the same workToken AND cacheToken, the {@link Work} is a duplicate. If multiple units of {@link
 * Work} have the same workToken, but different cacheTokens, the {@link Work} is a retry. If
 * multiple units of {@link Work} have the same cacheToken, but different workTokens, the {@link
 * Work} is obsolete.
 */
@AutoValue
public abstract class WorkId {

  public static Builder builder() {
    return new AutoValue_WorkId.Builder();
  }

  public static WorkId of(Windmill.WorkItem workItem) {
    return WorkId.builder()
        .setCacheToken(workItem.getCacheToken())
        .setWorkToken(workItem.getWorkToken())
        .build();
  }

  public abstract long cacheToken();

  public abstract long workToken();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCacheToken(long value);

    public abstract Builder setWorkToken(long value);

    public abstract WorkId build();
  }
}
