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
 * {@code InputT} and is {@link Serializable}.
 *
 * <p>This is the most general function type provided in this SDK, allowing arbitrary {@code
 * Exception}s to be thrown, and matching Java's expectations of a <i>functional interface</i> that
 * can be supplied as a lambda expression or method reference. It is named {@code ProcessFunction}
 * because it is particularly appropriate anywhere a user needs to provide code that will eventually
 * be executed as part of a {@link DoFn} {@link org.apache.beam.sdk.transforms.DoFn.ProcessElement
 * ProcessElement} function, which is allowed to declare throwing {@code Exception}. If you need to
 * execute user code in a context where arbitrary checked exceptions should not be allowed, require
 * that users implement the subinterface {@link SerializableFunction} instead.
 *
 * <p>For more robust {@link org.apache.beam.sdk.coders.Coder Coder} inference, consider extending
 * {@link InferableFunction} rather than implementing this interface directly.
 *
 * @param <InputT> input value type
 * @param <OutputT> output value type
 */
@FunctionalInterface
public interface ProcessFunction<InputT, OutputT> extends Serializable {
  /** Returns the result of invoking this function on the given input. */
  OutputT apply(InputT input) throws Exception;
}
