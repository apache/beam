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

import java.io.Serializable;

/**
 * A function that computes an output value of type {@code OutputT} from an input value of type
 * {@code InputT}, is {@link Serializable}, and does not allow checked exceptions to be declared.
 *
 * <p>To allow checked exceptions, implement the superinterface {@link ProcessFunction} instead. To
 * allow more robust {@link org.apache.beam.sdk.coders.Coder Coder} inference, see {@link
 * InferableFunction}.
 *
 * @param <InputT> input value type
 * @param <OutputT> output value type
 */
@FunctionalInterface
public interface SerializableFunction<InputT, OutputT>
    extends ProcessFunction<InputT, OutputT>, Serializable {
  /** Returns the result of invoking this function on the given input. */
  @Override
  OutputT apply(InputT input);
}
