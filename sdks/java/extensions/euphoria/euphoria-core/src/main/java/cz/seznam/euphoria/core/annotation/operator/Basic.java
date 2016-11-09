
package cz.seznam.euphoria.core.annotation.operator;

import java.lang.annotation.Documented;

/**
 * Annotation marking an operator a {@code Basic} operator.
 * A basic operator is such operator that an executor *must* implement
 * in order to be able to run any flow.
 */
@Documented
public @interface Basic {
    
}
