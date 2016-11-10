
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
