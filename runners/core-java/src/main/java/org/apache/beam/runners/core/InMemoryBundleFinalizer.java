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
package org.apache.beam.runners.core;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/** An in-memory {@link BundleFinalizer} that keeps track of any pending finalization requests. */
public class InMemoryBundleFinalizer implements BundleFinalizer {
  private final List<Finalization> requestedFinalizations = new ArrayList<>();

  @AutoValue
  public abstract static class Finalization {
    public static Finalization of(Instant expiryTime, Callback callback) {
      return new AutoValue_InMemoryBundleFinalizer_Finalization(expiryTime, callback);
    }

    public abstract Instant getExpiryTime();

    public abstract Callback getCallback();
  }

  @Override
  public void afterBundleCommit(Instant callbackExpiry, Callback callback) {
    requestedFinalizations.add(Finalization.of(callbackExpiry, callback));
  }

  /** Returns any pending finalizations clearing internal state. */
  public List<Finalization> getAndClearFinalizations() {
    List<Finalization> rval = ImmutableList.copyOf(requestedFinalizations);
    requestedFinalizations.clear();
    return rval;
  }
}
