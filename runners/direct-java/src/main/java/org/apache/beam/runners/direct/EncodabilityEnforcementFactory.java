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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

/**
 * Enforces that all elements in a {@link PCollection} can be encoded using that
 * {@link PCollection PCollection's} {@link Coder}.
 */
class EncodabilityEnforcementFactory implements ModelEnforcementFactory {
  // The factory proper is stateless
  private static final EncodabilityEnforcementFactory INSTANCE =
      new EncodabilityEnforcementFactory();

  public static EncodabilityEnforcementFactory create() {
    return INSTANCE;
  }

  @Override
  public <T> ModelEnforcement<T> forBundle(
      CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
    return new EncodabilityEnforcement<>();
  }

  private static class EncodabilityEnforcement<T> extends AbstractModelEnforcement<T> {
    @Override
    public void afterFinish(
        CommittedBundle<T> input,
        TransformResult result,
        Iterable<? extends CommittedBundle<?>> outputs) {
      for (CommittedBundle<?> bundle : outputs) {
        ensureBundleEncodable(bundle);
      }
    }

    private <T> void ensureBundleEncodable(CommittedBundle<T> bundle) {
      Coder<T> coder = bundle.getPCollection().getCoder();
      for (WindowedValue<T> element : bundle.getElements()) {
        try {
          T clone = CoderUtils.clone(coder, element.getValue());
          if (coder.consistentWithEquals()) {
            checkArgument(
                coder.structuralValue(element.getValue()).equals(coder.structuralValue(clone)),
                "Coder %s of class %s does not maintain structural value equality"
                    + " on input element %s",
                coder,
                coder.getClass().getSimpleName(),
                element.getValue());
          }
        } catch (Exception e) {
          throw UserCodeException.wrap(e);
        }
      }
    }
  }
}
