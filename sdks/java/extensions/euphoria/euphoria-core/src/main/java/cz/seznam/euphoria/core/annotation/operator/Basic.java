
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
