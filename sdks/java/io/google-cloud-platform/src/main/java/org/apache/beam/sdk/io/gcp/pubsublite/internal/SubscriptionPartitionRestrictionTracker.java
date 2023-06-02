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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.testing.SerializableMatchers.SerializableSupplier;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class SubscriptionPartitionRestrictionTracker extends RestrictionTracker<Integer, Integer>
    implements HasProgress {
  private boolean terminated = false;
  private int position;
  private final SerializableSupplier<Boolean> terminate;

  public SubscriptionPartitionRestrictionTracker(
      int input, SerializableSupplier<Boolean> terminate) {
    this.position = input;
    this.terminate = terminate;
  }

  @Override
  public boolean tryClaim(Integer newPosition) {
    checkArgument(newPosition >= position);
    if (terminated) {
      return false;
    }
    if (terminate.get()) {
      terminated = true;
      return false;
    }
    position = newPosition;
    return true;
  }

  @Override
  public Integer currentRestriction() {
    return position;
  }

  @Override
  public @Nullable SplitResult<Integer> trySplit(double fractionOfRemainder) {
    if (fractionOfRemainder != 0) {
      return null;
    }
    if (terminated) {
      return null;
    }
    terminated = true;
    return SplitResult.of(position, position);
  }

  @Override
  public void checkDone() throws IllegalStateException {
    checkState(terminated);
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Progress getProgress() {
    return Progress.from(position, position);
  }
}
