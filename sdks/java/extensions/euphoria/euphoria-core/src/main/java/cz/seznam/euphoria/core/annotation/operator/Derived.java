
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

  /** State complexity, use {@code StateComplexity.<value>}. */
  int state();

  /** Number of global repartition operations. */
  int repartitions();


}
