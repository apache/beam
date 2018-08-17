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
package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Calculate maximum.
 */
public class Max {

  /**
   * Return {@code CombinableReduceFunction} to calculate maximum of input.
   *
   * @param <IN> the type of elements handled
   * @param <X> the type of key by which to compare the elements
   *
   * @param extract the key extraction function
   *
   * @return a combiner function which delivers the "maximum" element seen;
   *          never {@code null}
   */
  public static <IN, X extends Comparable<X>> CombinableReduceFunction<IN> of(
      UnaryFunction<IN, X> extract) {
    
    return values -> {
      IN max = null;
      X maxValue = null;
      for (IN input : values) {
        X value = extract.apply(input);
        if (maxValue == null || maxValue.compareTo(value) < 0) {
          max = input;
          maxValue = value;
        }
      }
      return max;
    };
  }

  private Max() { }

}
