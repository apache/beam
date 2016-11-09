
package cz.seznam.euphoria.core.annotation.operator;

import cz.seznam.euphoria.core.client.operator.Operator;
import java.lang.annotation.Documented;

/**
 * A {@code Derived} operator is operator that is efficiently
 * implemented by the basic or recommended operators, so there
 * is no explicit reason for the executor to implement it by hand.
 */
@Documented
public @interface Derived {

  Class<? extends Operator>[] basic();

}
