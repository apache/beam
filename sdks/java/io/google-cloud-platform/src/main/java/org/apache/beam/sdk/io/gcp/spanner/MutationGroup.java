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
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A bundle of mutations that must be submitted atomically.
 *
 * <p>One of the mutations is chosen to be "primary", and can be used to determine partitions.
 */
public final class MutationGroup implements Serializable, Iterable<Mutation> {
  private final ImmutableList<Mutation> mutations;

  public static Builder withPrimary(Mutation primary) {
    return new Builder(primary);
  }

  @Override
  public Iterator<Mutation> iterator() {
    return mutations.iterator();
  }

  /** Builder for {@link org.apache.beam.sdk.io.gcp.spanner.MutationGroup}. */
  public static class Builder {
    private final ImmutableList.Builder<Mutation> builder;

    private Builder(Mutation primary) {
      this.builder = ImmutableList.<Mutation>builder().add(primary);
    }

    public Builder attach(Mutation m) {
      this.builder.add(m);
      return this;
    }

    public Builder attach(Iterable<Mutation> mutations) {
      this.builder.addAll(mutations);
      return this;
    }

    public Builder attach(Mutation... mutations) {
      this.builder.addAll(Arrays.asList(mutations));
      return this;
    }

    public MutationGroup build() {
      return new MutationGroup(builder.build());
    }
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
}
