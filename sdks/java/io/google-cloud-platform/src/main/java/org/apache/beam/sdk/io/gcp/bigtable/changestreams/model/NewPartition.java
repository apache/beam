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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

/**
 * Represent new partition as a result of splits and merges.
 *
 * <p>Parent partitions stop streaming and record their change stream continuation tokens. The
 * tokens are collected to continue the stream from the new partition.
 */
@Internal
public class NewPartition implements Serializable {
  private static final long serialVersionUID = -6530725494713332884L;
  ByteStringRange partition;
  List<ChangeStreamContinuationToken> changeStreamContinuationTokens;
  Instant lowWatermark;
  Instant lastUpdated;

  public NewPartition(
      ByteStringRange partition,
      List<ChangeStreamContinuationToken> changeStreamContinuationTokens,
      Instant lowWatermark,
      Instant lastUpdated) {
    this.partition = partition;
    this.changeStreamContinuationTokens = changeStreamContinuationTokens;
    this.lowWatermark = lowWatermark;
    this.lastUpdated = lastUpdated;
  }

  public NewPartition(
      ByteStringRange partition,
      List<ChangeStreamContinuationToken> changeStreamContinuationTokens,
      Instant lowWatermark) {
    this(partition, changeStreamContinuationTokens, lowWatermark, Instant.EPOCH);
  }

  public ByteStringRange getPartition() {
    return partition;
  }

  public List<ChangeStreamContinuationToken> getChangeStreamContinuationTokens() {
    return changeStreamContinuationTokens;
  }

  public Instant getLowWatermark() {
    return lowWatermark;
  }

  public List<ByteStringRange> getParentPartitions() {
    return getChangeStreamContinuationTokens().stream()
        .map(ChangeStreamContinuationToken::getPartition)
        .collect(Collectors.toList());
  }

  public Instant getLastUpdated() {
    return lastUpdated;
  }

  /**
   * Return a new NewPartition that only contains one token that matches the parentPartition. If the
   * parentPartition does not exist, return null.
   *
   * @param parentPartition to parent partition to find within the tokens.
   * @return a NewPartition with one token with parentPartition. null otherwise.
   */
  public @Nullable NewPartition getSingleTokenNewPartition(ByteStringRange parentPartition) {
    for (ChangeStreamContinuationToken parentToken : getChangeStreamContinuationTokens()) {
      if (parentToken.getPartition().equals(parentPartition)) {
        // The partition stays the same for the splices because that's the row key to find the cells
        // to clean up.
        return new NewPartition(
            getPartition(),
            Collections.singletonList(parentToken),
            getLowWatermark(),
            getLastUpdated());
      }
    }
    return null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NewPartition)) {
      return false;
    }
    NewPartition that = (NewPartition) o;
    return Objects.equals(getPartition(), that.getPartition())
        && Objects.equals(
            getChangeStreamContinuationTokens(), that.getChangeStreamContinuationTokens())
        && Objects.equals(getLowWatermark(), that.getLowWatermark())
        && Objects.equals(getLastUpdated(), that.getLastUpdated());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getPartition(), getChangeStreamContinuationTokens(), getLowWatermark(), getLastUpdated());
  }

  @Override
  public String toString() {
    return "NewPartition{"
        + "partition="
        + formatByteStringRange(partition)
        + ", changeStreamContinuationTokens="
        + changeStreamContinuationTokens.stream()
            .map(t -> formatByteStringRange(t.getPartition()) + " => {" + t.getToken() + "}")
            .collect(Collectors.joining(", ", "{", "}"))
        + ", lowWatermark="
        + lowWatermark
        + ", lastUpdated="
        + lastUpdated
        + '}';
  }
}
