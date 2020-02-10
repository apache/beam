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
package org.apache.beam.runners.core.construction;

import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Features a {@link DoFn} can posses. Each runner might implement a different (sub)set of this
 * features.
 */
public class DoFnFeatures {

  public static boolean isSplittable(DoFn<?, ?> doFn) {
    return DoFnSignatures.signatureForDoFn(doFn).processElement().isSplittable();
  }

  public static boolean isStateful(DoFn<?, ?> doFn) {
    return usesState(doFn) || usesTimers(doFn);
  }

  public static boolean usesMapState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, MapState.class);
  }

  public static boolean usesSetState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, SetState.class);
  }

  public static boolean usesValueState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, ValueState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesBagState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, BagState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesWatermarkHold(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, WatermarkHoldState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesTimers(DoFn<?, ?> doFn) {
    return DoFnSignatures.signatureForDoFn(doFn).usesTimers() || requiresTimeSortedInput(doFn);
  }

  public static boolean usesState(DoFn<?, ?> doFn) {
    return DoFnSignatures.signatureForDoFn(doFn).usesState() || requiresTimeSortedInput(doFn);
  }

  public static boolean requiresTimeSortedInput(DoFn<?, ?> doFn) {
    return DoFnSignatures.signatureForDoFn(doFn).processElement().requiresTimeSortedInput();
  }

  private static boolean usesGivenStateClass(DoFn<?, ?> doFn, Class<? extends State> stateClass) {
    return DoFnSignatures.signatureForDoFn(doFn).stateDeclarations().values().stream()
        .anyMatch(d -> d.stateType().isSubtypeOf(TypeDescriptor.of(stateClass)));
  }

  private DoFnFeatures() {}
}
