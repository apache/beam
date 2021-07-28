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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

@DefaultCoder(AvroCoder.class)
public class ChildPartitionsRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 5442772555232576887L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;

  private String recordSequence;
  private List<ChildPartition> childPartitions;
  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private ChildPartitionsRecord() {}

  @VisibleForTesting
  ChildPartitionsRecord(
      Timestamp startTimestamp,
      String recordSequence,
      List<ChildPartition> childPartitions,
      ChangeStreamRecordMetadata metadata) {
    this.startTimestamp = startTimestamp;
    this.recordSequence = recordSequence;
    this.childPartitions = childPartitions;
    this.metadata = metadata;
  }

  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  public String getRecordSequence() {
    return recordSequence;
  }

  public List<ChildPartition> getChildPartitions() {
    return childPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChildPartitionsRecord)) {
      return false;
    }
    ChildPartitionsRecord that = (ChildPartitionsRecord) o;
    return Objects.equals(startTimestamp, that.startTimestamp)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(childPartitions, that.childPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimestamp, recordSequence, childPartitions);
  }

  @Override
  public String toString() {
    return "ChildPartitionsRecord{"
        + "startTimestamp="
        + startTimestamp
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", childPartitions="
        + childPartitions
        + ", metadata="
        + metadata
        + '}';
  }

  @DefaultCoder(AvroCoder.class)
  public static class ChildPartition implements Serializable {

    private static final long serialVersionUID = -650413326832931368L;
    private String token;
    // This needs to be an implementation (HashSet), instead of the Set interface, otherwise
    // we can not encode / decode this with Avro.
    private HashSet<String> parentTokens;

    private ChildPartition() {}

    public ChildPartition(String token, HashSet<String> parentTokens) {
      this.token = token;
      this.parentTokens = parentTokens;
    }

    public ChildPartition(String token, String parentToken) {
      this(token, Sets.newHashSet(parentToken));
    }

    public String getToken() {
      return token;
    }

    public HashSet<String> getParentTokens() {
      return parentTokens;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ChildPartition)) {
        return false;
      }
      ChildPartition that = (ChildPartition) o;
      return Objects.equals(token, that.token) && Objects.equals(parentTokens, that.parentTokens);
    }

    @Override
    public int hashCode() {
      return Objects.hash(token, parentTokens);
    }

    @Override
    public String toString() {
      return "ChildPartition{"
          + "childToken='"
          + token
          + '\''
          + ", parentTokens="
          + parentTokens
          + '}';
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Timestamp startTimestamp;
    private String recordSequence;
    private List<ChildPartition> childPartitions;
    private ChangeStreamRecordMetadata metadata;

    public Builder withStartTimestamp(Timestamp startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    public Builder withRecordSequence(String recordSequence) {
      this.recordSequence = recordSequence;
      return this;
    }

    public Builder withChildPartitions(List<ChildPartition> childPartitions) {
      this.childPartitions = childPartitions;
      return this;
    }

    public Builder withMetadata(ChangeStreamRecordMetadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public ChildPartitionsRecord build() {
      return new ChildPartitionsRecord(startTimestamp, recordSequence, childPartitions, metadata);
    }
  }
}
