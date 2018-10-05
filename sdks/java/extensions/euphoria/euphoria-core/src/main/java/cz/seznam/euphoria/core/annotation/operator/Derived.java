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
package cz.seznam.euphoria.core.annotation.operator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * A {@code Derived} operator is operator that is efficiently
 * implemented by the basic or recommended operators, so there
 * is no explicit reason for the executor to implement it by hand.
 */
@Documented
@Target(ElementType.TYPE)
public @interface Derived {

  /** @return the state complexity */
  StateComplexity state();

  /** @return the number of global repartition operations */
  int repartitions();


}
