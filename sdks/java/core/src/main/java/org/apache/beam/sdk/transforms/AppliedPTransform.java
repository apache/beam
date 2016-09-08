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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
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
public class AppliedPTransform
    <InputT extends PInput, OutputT extends POutput,
     TransformT extends PTransform<? super InputT, OutputT>> {

  private final String fullName;
  private final InputT input;
  private final OutputT output;
  private final TransformT transform;

  private AppliedPTransform(String fullName, InputT input, OutputT output, TransformT transform) {
    this.input = input;
    this.output = output;
    this.transform = transform;
    this.fullName = fullName;
  }

  public static <InputT extends PInput, OutputT extends POutput,
                 TransformT extends PTransform<? super InputT, OutputT>>
  AppliedPTransform<InputT, OutputT, TransformT> of(
      String fullName, InputT input, OutputT output, TransformT transform) {
    return new AppliedPTransform<InputT, OutputT, TransformT>(fullName, input, output, transform);
  }

  public String getFullName() {
    return fullName;
  }

  public InputT getInput() {
    return input;
  }

  public OutputT getOutput() {
    return output;
  }

  public TransformT getTransform() {
    return transform;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFullName(), getInput(), getOutput(), getTransform());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AppliedPTransform) {
      AppliedPTransform<?, ?, ?> that = (AppliedPTransform<?, ?, ?>) other;
      return Objects.equal(this.getFullName(), that.getFullName())
          && Objects.equal(this.getInput(), that.getInput())
          && Objects.equal(this.getOutput(), that.getOutput())
          && Objects.equal(this.getTransform(), that.getTransform());
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("fullName", getFullName())
        .add("input", getInput())
        .add("output", getOutput())
        .add("transform", getTransform())
        .toString();
  }
}
