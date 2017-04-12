/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
 * Function of single argument.
 *
 * @param <IN> the type of the element processed
 * @param <OUT> the type of the result applying element to the function
 */
@FunctionalInterface
public interface UnaryFunction<IN, OUT> extends Serializable {
  
  /**
   * Return the result of this function.
   *
   * @param what the element applied to the function
   *
   * @return the result of the function application
   */
  OUT apply(IN what);

}
