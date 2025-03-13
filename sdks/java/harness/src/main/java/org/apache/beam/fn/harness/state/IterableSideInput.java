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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.fn.harness.Cache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materializations.IterableView;

/**
 * An implementation of a iterable side input that utilizes the Beam Fn State API to fetch values.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class IterableSideInput<T> implements IterableView<T> {

  private final Iterable<T> values;

  public IterableSideInput(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<T> valueCoder) {
    checkArgument(
        stateKey.hasIterableSideInput(),
        "Expected IterableSideInput StateKey but received %s.",
        stateKey);
    this.values =
        StateFetchingIterators.readAllAndDecodeStartingFrom(
            cache,
            beamFnStateClient,
            StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build(),
            valueCoder);
  }

  @Override
  public Iterable<T> get() {
    return values;
  }
}
