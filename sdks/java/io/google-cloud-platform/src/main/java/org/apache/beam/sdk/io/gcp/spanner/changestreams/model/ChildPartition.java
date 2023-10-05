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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/**
 * A child partition represents a new partition that should be queried. Child partitions are emitted
 * in {@link ChildPartitionsRecord}.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class ChildPartition implements Serializable {

  private static final long serialVersionUID = -650413326832931368L;

  private String token;
  // This needs to be an implementation (HashSet), instead of the Set interface, otherwise
  // we can not encode / decode this.
  private HashSet<String> parentTokens;

  /** Default constructor for serialization only. */
  private ChildPartition() {}

  /**
   * Constructs a child partition, which will have its own token and the parents that it originated
   * from. A child partition will have a single parent if it is originated from a partition move or
   * split. A child partition will have multiple parents if it is originated from a partition merge.
   *
   * @param token the child partition token
   * @param parentTokens the partition tokens of the parent(s) that originated the child partition
   */
  public ChildPartition(String token, HashSet<String> parentTokens) {
    this.token = token;
    this.parentTokens = parentTokens;
  }

  /**
   * Constructs a child partition, which will have its own token and the parent that it originated
   * from. Use this constructor for child partitions with a single parent (generated from a move or
   * split). If a child partition has multiple parents use the constructor {@link
   * ChildPartition(String, HashSet)}.
   *
   * @param token the child partition token
   * @param parentToken the partition tokens of the parent that originated the child partition
   */
  public ChildPartition(String token, String parentToken) {
    this(token, Sets.newHashSet(parentToken));
  }

  /**
   * Unique partition identifier, which can be used to perform a change stream query.
   *
   * @return the unique partition identifier
   */
  public String getToken() {
    return token;
  }

  /**
   * The unique partition identifiers of the parent partitions where this child partition originated
   * from.
   *
   * @return a set of parent partition tokens
   */
  public HashSet<String> getParentTokens() {
    return parentTokens;
  }

  @Override
  public boolean equals(@Nullable Object o) {
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
