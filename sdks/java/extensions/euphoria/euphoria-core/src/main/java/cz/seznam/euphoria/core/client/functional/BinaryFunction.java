/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Function of two arguments.
 *
 * @param <LEFT> the type of the left operand
 * @param <RIGHT> the type of the right operand
 * @param <OUT> the type of the result of the function application
 */
@FunctionalInterface
public interface BinaryFunction<LEFT, RIGHT, OUT> extends Serializable {


  /**
   * Applies this function to the given arguments.
   *
   * @param left the "left" parameter of the operation
   * @param right the "right" parameter of the operation
   *
   * @return the result of applying "left" and "right" to an operation
   */
  OUT apply(LEFT left, RIGHT right);

}
