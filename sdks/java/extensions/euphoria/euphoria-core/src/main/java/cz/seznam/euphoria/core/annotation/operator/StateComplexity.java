/**
 * Copyright 2016 Seznam a.s.
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

package cz.seznam.euphoria.core.annotation.operator;

/**
 * Space complexity of operator's state in each window depending on size of input.
 */
public class StateComplexity {

  /** The size of state will be O(N) in the size of input. */
  public static final int LINEAR = 1;

  /** The size of state will be sub-linear but not constant. */
  public static final int SUBLINEAR = 2;

  /** The size of state will be O(1) in the size of input. */
  public static final int CONSTANT = 3;
  
  /** There is no state in this operator. */
  public static final int ZERO = 4;

  /**
   * The size of state will be O(1) if the passed function
   * is `combinable` (commutative, associative), otherwise it will be O(N).
   */
  public static final int CONSTANT_IF_COMBINABLE = 5;

  // do not construct this object
  private StateComplexity() { }

}
