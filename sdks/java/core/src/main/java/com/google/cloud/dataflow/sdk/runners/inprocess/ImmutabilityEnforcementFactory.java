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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.IllegalMutationException;
import com.google.cloud.dataflow.sdk.util.MutationDetector;
import com.google.cloud.dataflow.sdk.util.MutationDetectors;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * {@link ModelEnforcement} that enforces elements are not modified over the course of processing
 * an element.
 *
 * <p>Implies {@link EncodabilityEnforcment}.
 */
class ImmutabilityEnforcementFactory implements ModelEnforcementFactory {
  public static ModelEnforcementFactory create() {
    return new ImmutabilityEnforcementFactory();
  }

  @Override
  public <T> ModelEnforcement<T> forBundle(
      CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
    return new ImmutabilityCheckingEnforcement<T>(input, consumer);
  }

  private static class ImmutabilityCheckingEnforcement<T> extends AbstractModelEnforcement<T> {
    private final AppliedPTransform<?, ?, ?> transform;
    private final Map<WindowedValue<T>, MutationDetector> mutationElements;
    private final Coder<T> coder;

    private ImmutabilityCheckingEnforcement(
        CommittedBundle<T> input, AppliedPTransform<?, ?, ?> transform) {
      this.transform = transform;
      coder = SerializableUtils.clone(input.getPCollection().getCoder());
      mutationElements = new IdentityHashMap<>();
    }

    @Override
    public void beforeElement(WindowedValue<T> element) {
      try {
        mutationElements.put(
            element, MutationDetectors.forValueWithCoder(element.getValue(), coder));
      } catch (CoderException e) {
        throw UserCodeException.wrap(e);
      }
    }

    @Override
    public void afterElement(WindowedValue<T> element) {
      verifyUnmodified(mutationElements.get(element));
    }

    @Override
    public void afterFinish(
        CommittedBundle<T> input,
        InProcessTransformResult result,
        Iterable<? extends CommittedBundle<?>> outputs) {
      for (MutationDetector detector : mutationElements.values()) {
        verifyUnmodified(detector);
      }
    }

    private void verifyUnmodified(MutationDetector detector) {
      try {
        detector.verifyUnmodified();
      } catch (IllegalMutationException e) {
        throw new IllegalMutationException(
            String.format(
                "PTransform %s illegaly mutated value %s of class %s."
                    + " Input values must not be mutated in any way.",
                transform.getFullName(),
                e.getSavedValue(),
                e.getSavedValue().getClass()),
            e.getSavedValue(),
            e.getNewValue());
      }
    }
  }
}
