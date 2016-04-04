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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Enforces that all elements in a {@link PCollection} can be encoded using that
 * {@link PCollection PCollection's} {@link Coder}.
 */
class EncodabilityEnforcementFactory implements ModelEnforcementFactory {
  public static EncodabilityEnforcementFactory create() {
    return new EncodabilityEnforcementFactory();
  }

  @Override
  public <T> ModelEnforcement<T> forBundle(
      CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
    return new EncodabilityEnforcement<>(input);
  }

  private static class EncodabilityEnforcement<T> extends AbstractModelEnforcement<T> {
    private Coder<T> coder;

    public EncodabilityEnforcement(CommittedBundle<T> input) {
      coder = SerializableUtils.clone(input.getPCollection().getCoder());
    }

    @Override
    public void beforeElement(WindowedValue<T> element) {
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
