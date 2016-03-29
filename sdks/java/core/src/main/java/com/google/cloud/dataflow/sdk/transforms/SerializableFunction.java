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

import java.io.Serializable;

/**
 * A function that computes an output value of type {@code OutputT} from an input value of type
 * {@code InputT} and is {@link Serializable}.
 *
 * @param <InputT> input value type
 * @param <OutputT> output value type
 */
public interface SerializableFunction<InputT, OutputT> extends Serializable {
  /** Returns the result of invoking this function on the given input. */
  public OutputT apply(InputT input);
}
