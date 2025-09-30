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
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
@AutoValue.CopyAnnotations
@Internal
abstract class SplitResultsWithStopIndex {
  public static SplitResultsWithStopIndex of(
      WindowedSplitResult windowSplit,
      HandlesSplits.@Nullable SplitResult downstreamSplit,
      int newWindowStopIndex) {
    return new AutoValue_SplitResultsWithStopIndex(
        windowSplit, downstreamSplit, newWindowStopIndex);
  }

  @Pure
  public abstract WindowedSplitResult getWindowSplit();

  @Pure
  public abstract HandlesSplits.@Nullable SplitResult getDownstreamSplit();

  @Pure
  public abstract int getNewWindowStopIndex();
}
