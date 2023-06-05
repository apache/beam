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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

@Internal
public class StreamPartitionWithWatermark implements Serializable {
  private static final long serialVersionUID = -318960470803696894L;

  final ByteStringRange partition;
  final Instant watermark;

  public StreamPartitionWithWatermark(ByteStringRange partition, Instant watermark) {
    this.partition = partition;
    this.watermark = watermark;
  }

  public ByteStringRange getPartition() {
    return partition;
  }

  public Instant getWatermark() {
    return watermark;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamPartitionWithWatermark)) {
      return false;
    }
    StreamPartitionWithWatermark that = (StreamPartitionWithWatermark) o;
    return Objects.equals(getPartition(), that.getPartition())
        && Objects.equals(getWatermark(), that.getWatermark());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPartition(), getWatermark());
  }

  @Override
  public String toString() {
    return "StreamPartitionWithWatermark{"
        + "partition="
        + formatByteStringRange(partition)
        + ", watermark="
        + watermark
        + '}';
  }
}
