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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation marking an operator a {@code Basic} operator.
 * A basic operator is such operator that an executor *must* implement
 * in order to be able to run any flow.
 */
@Documented
@Target(ElementType.TYPE)
public @interface Basic {

  /** State complexity, use {@code StateComplexity.<value>}. */
  int state();
  
  /** Number of global repartition operations. */
  int repartitions();
    
}
