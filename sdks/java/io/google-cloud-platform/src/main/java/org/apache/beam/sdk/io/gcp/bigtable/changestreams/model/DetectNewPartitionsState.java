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
 * Metadata of the progress of {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn} from the metadata
 * table.
 */
@Internal
public class DetectNewPartitionsState implements Serializable {
  private static final long serialVersionUID = 6587699311321738060L;

  private final Instant watermark;
  private final Instant watermarkLastUpdated;

  public DetectNewPartitionsState(Instant watermark, Instant watermarkLastUpdated) {
    this.watermark = watermark;
    this.watermarkLastUpdated = watermarkLastUpdated;
  }

  public Instant getWatermark() {
    return watermark;
  }

  public Instant getWatermarkLastUpdated() {
    return watermarkLastUpdated;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DetectNewPartitionsState)) {
      return false;
    }
    DetectNewPartitionsState that = (DetectNewPartitionsState) o;
    return Objects.equals(getWatermark(), that.getWatermark())
        && Objects.equals(getWatermarkLastUpdated(), that.getWatermarkLastUpdated());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getWatermark(), getWatermarkLastUpdated());
  }

  @Override
  public String toString() {
    return "DetectNewPartitions{"
        + "watermark="
        + watermark
        + ", watermarkLastUpdated="
        + watermarkLastUpdated
        + '}';
  }
}
