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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import com.google.cloud.Timestamp;
import java.io.Serializable;

/** The PartitionRestrictionMetadata class. */
@SuppressWarnings("initialization.fields.uninitialized")
public class PartitionRestrictionMetadata implements Serializable {

  private static final long serialVersionUID = 4744539370804123350L;

  private final String partitionToken;
  private final Timestamp partitionStartTimestamp;
  private final Timestamp partitionEndTimestamp;

  public PartitionRestrictionMetadata(
      String partitionToken, Timestamp partitionStartTimestamp, Timestamp partitionEndTimestamp) {
    this.partitionToken = partitionToken;
    this.partitionStartTimestamp = partitionStartTimestamp;
    this.partitionEndTimestamp = partitionEndTimestamp;
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public Timestamp getPartitionStartTimestamp() {
    return partitionStartTimestamp;
  }

  public Timestamp getPartitionEndTimestamp() {
    return partitionEndTimestamp;
  }

  @Override
  public String toString() {
    return "PartitionRestrictionMetadata{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", partitionStartTimestamp="
        + partitionStartTimestamp
        + ", partitionEndTimestamp="
        + partitionEndTimestamp
        + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(PartitionRestrictionMetadata metadata) {
    return new Builder(metadata);
  }

  public static class Builder {
    private String partitionToken;
    private Timestamp partitionStartTimestamp;
    private Timestamp partitionEndTimestamp;

    public Builder() {}

    public Builder(PartitionRestrictionMetadata metadata) {
      this.partitionToken = metadata.partitionToken;
      this.partitionStartTimestamp = metadata.partitionStartTimestamp;
      this.partitionEndTimestamp = metadata.partitionEndTimestamp;
    }

    public Builder withPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    public Builder withPartitionStartTimestamp(Timestamp partitionStartTimestamp) {
      this.partitionStartTimestamp = partitionStartTimestamp;
      return this;
    }

    public Builder withPartitionEndTimestamp(Timestamp partitionEndTimestamp) {
      this.partitionEndTimestamp = partitionEndTimestamp;
      return this;
    }

    public PartitionRestrictionMetadata build() {
      return new PartitionRestrictionMetadata(
          partitionToken, partitionStartTimestamp, partitionEndTimestamp);
    }
  }
}
