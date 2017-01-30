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
package cz.seznam.euphoria.core.annotation.operator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * A {@code Recommended} operator is such an operator that is strongly
 * advised to be implemented natively by executor due to performance
 * reasons.
 */
@Documented
@Target(ElementType.TYPE)
public @interface Recommended {

  /** Textual documentation of the reason of the recommendation. */
  String reason();

  /** State complexity, use {@code StateComplexity.<value>}. */
  int state();

  /** Number of global repartition operations. */
  int repartitions();
  
}
