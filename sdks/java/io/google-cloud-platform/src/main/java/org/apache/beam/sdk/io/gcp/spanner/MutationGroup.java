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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A bundle of mutations that must be submitted atomically.
 *
 * <p>One of the mutations is chosen to be "primary", and can be used to determine partitions.
 */
public final class MutationGroup implements Serializable, Iterable<Mutation> {
  private final ImmutableList<Mutation> mutations;

  /**
   * Creates a new group.
   *
   * @param primary a primary mutation.
   * @param other other mutations, usually interleaved in parent.
   * @return new mutation group.
   */
  public static MutationGroup create(Mutation primary, Mutation... other) {
    return create(primary, Arrays.asList(other));
  }

  public static MutationGroup create(Mutation primary, Iterable<Mutation> other) {
    return new MutationGroup(ImmutableList.<Mutation>builder().add(primary).addAll(other).build());
  }

  @Override
  public Iterator<Mutation> iterator() {
    return mutations.iterator();
  }

  public long size() {
    return mutations.size();
  }

  private MutationGroup(ImmutableList<Mutation> mutations) {
    this.mutations = mutations;
  }

  public Mutation primary() {
    return mutations.get(0);
  }

  public List<Mutation> attached() {
    return mutations.subList(1, mutations.size());
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MutationGroup mutations1 = (MutationGroup) o;
    return Objects.equal(mutations, mutations1.mutations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mutations);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MutationGroup(");
    sb.append(mutations);
    sb.append(")");
    return sb.toString();
  }
}
