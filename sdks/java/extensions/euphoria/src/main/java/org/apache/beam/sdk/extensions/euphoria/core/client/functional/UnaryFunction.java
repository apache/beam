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
package org.apache.beam.sdk.extensions.euphoria.core.client.functional;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/**
 * Function of single argument.
 *
 * @param <InputT> the type of the element processed
 * @param <OutputT> the type of the result applying element to the function
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface UnaryFunction<InputT, OutputT> extends Serializable {

  /**
   * Return the result of this function.
   *
   * @param what the element applied to the function
   * @return the result of the function application
   */
  OutputT apply(InputT what);

  /**
   * Returns a {@link UnaryFunction} that always returns its input argument.
   *
   * @param <T> the type of the input and output objects to the function
   * @return a function that always returns its input argument
   */
  static <T> UnaryFunction<T, T> identity() {
    return t -> t;
  }
}
