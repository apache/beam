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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * Represents the application of a {@link PTransform} to a specific input to produce
 * a specific output.
 *
 * <p>For internal use.
 *
 * @param <InputT> transform input type
 * @param <OutputT> transform output type
 * @param <TransformT> transform type
 */
@AutoValue
public abstract class AppliedPTransform
    <InputT extends PInput, OutputT extends POutput,
     TransformT extends PTransform<? super InputT, OutputT>> {

  public static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<? super InputT, OutputT>>
      AppliedPTransform<InputT, OutputT, TransformT> of(
          String fullName, InputT input, OutputT output, TransformT transform) {
    return new AutoValue_AppliedPTransform<InputT, OutputT, TransformT>(
        fullName, input, output, transform);
  }

  public abstract String getFullName();

  public abstract InputT getInput();

  public abstract OutputT getOutput();

  public abstract TransformT getTransform();
}
