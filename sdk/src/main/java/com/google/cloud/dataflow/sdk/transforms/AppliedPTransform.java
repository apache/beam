/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;

/**
 * Represents the application of this transform to a specific input to produce
 * a specific output.
 *
 * @param <Input> transform input type
 * @param <Output> transform output type
 * @param <PT> transform type
 */
public class AppliedPTransform
    <Input extends PInput, Output extends POutput, PT extends PTransform<Input, Output>> {
  public final Input input;
  public final Output output;
  public final PT transform;
  public AppliedPTransform(Input input, Output output, PT transform) {
    this.input = input;
    this.output = output;
    this.transform = transform;
  }

  public static <Input extends PInput, Output extends POutput, PT extends PTransform<Input, Output>>
  AppliedPTransform<Input, Output, PT> of(Input input, Output output, PT transform) {
    return new AppliedPTransform<Input, Output, PT>(input, output, transform);
  }
}
