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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.model;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * States to initialize a pipeline outputted by {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.InitializeDoFn}.
 */
@Internal
public class InitialPipelineState implements Serializable {
  private static final long serialVersionUID = 7685843906645495071L;
  private final Instant startTime;
  private final boolean resume;

  public InitialPipelineState(Instant startTime, boolean resume) {
    this.startTime = startTime;
    this.resume = resume;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public boolean isResume() {
    return resume;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InitialPipelineState)) {
      return false;
    }
    InitialPipelineState that = (InitialPipelineState) o;
    return isResume() == that.isResume() && Objects.equals(getStartTime(), that.getStartTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStartTime(), isResume());
  }

  @Override
  public String toString() {
    return "InitialPipeline{" + "startTime=" + startTime + ", resume=" + resume + '}';
  }
}
