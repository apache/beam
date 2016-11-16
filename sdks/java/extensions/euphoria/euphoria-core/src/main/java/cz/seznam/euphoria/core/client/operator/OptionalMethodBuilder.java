
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import java.util.Objects;

/**
 * Class to be extended by operator builders that want to make use
 * of `applyIf` call.
 *
 * @param <BUILDER> the class of the builder that extends this class
 */
public interface OptionalMethodBuilder<BUILDER> {

  @SuppressWarnings("unchecked")
  default BUILDER applyIf(boolean cond, UnaryFunction<BUILDER, BUILDER> apply) {
    Objects.requireNonNull(apply);
    return cond ? apply.apply((BUILDER) this) : (BUILDER) this;
  }

}
