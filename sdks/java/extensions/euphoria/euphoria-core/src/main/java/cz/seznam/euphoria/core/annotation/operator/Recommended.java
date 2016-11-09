
package cz.seznam.euphoria.core.annotation.operator;

import cz.seznam.euphoria.core.client.operator.Operator;
import java.lang.annotation.Documented;

/**
 * A {@code Recommended} operator is such an operator that is strongly
 * advised to be implemented natively by executor due to performance
 * reasons.
 */
@Documented
public @interface Recommended {

  /** Textual documentation of the reason of the recommendation. */
  String reason();

  /** List of basic operators. */
  Class<? extends Operator>[] basic();
  
}
