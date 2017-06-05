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

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A base class for implementing {@link PTransform} overrides, which behave identically to the
 * delegate transform but with overridden methods. Implementors are required to implement
 * {@link #delegate()}, which returns the object to forward calls to, and {@link #expand(PInput)}.
 */
public abstract class ForwardingPTransform<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {
  protected abstract PTransform<InputT, OutputT> delegate();

  @Override
  public OutputT expand(InputT input) {
    return delegate().expand(input);
  }

  @Override
  public void validate(PipelineOptions options) {
    delegate().validate(options);
  }

  @Override
  public String getName() {
    return delegate().getName();
  }

  @Override
  public <T> Coder<T> getDefaultOutputCoder(InputT input, PCollection<T> output)
      throws CannotProvideCoderException {
    return delegate().getDefaultOutputCoder(input, output);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.delegate(delegate());
  }
}
