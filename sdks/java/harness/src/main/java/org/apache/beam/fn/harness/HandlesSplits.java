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
package org.apache.beam.fn.harness;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

/**
 * An interface that may be used to extend a {@link FnDataReceiver} signalling that the downstream
 * runner is capable of performing splitting and providing progress reporting.
 */
public interface HandlesSplits {

  /** Returns null if the split was unsuccessful. */
  SplitResult trySplit(double fractionOfRemainder);

  /** Returns the current progress of the active element as a fraction between 0.0 and 1.0. */
  double getProgress();

  @AutoValue
  abstract class SplitResult {
    public static SplitResult of(
        List<BundleApplication> primaryRoots,
        List<BeamFnApi.DelayedBundleApplication> residualRoots) {
      return new AutoValue_HandlesSplits_SplitResult(primaryRoots, residualRoots);
    }

    public abstract List<BeamFnApi.BundleApplication> getPrimaryRoots();

    public abstract List<BeamFnApi.DelayedBundleApplication> getResidualRoots();
  }
}
