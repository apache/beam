
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;

/**
 * A composite key for operations applied on groups.
 */
class CompositeKey<T0, T1> extends Pair<T0, T1> {

  public static <T0, T1> CompositeKey<T0, T1> of(T0 first, T1 second) {
    return new CompositeKey<>(first, second);
  }

  CompositeKey(T0 first, T1 second) {
    super(first, second);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object obj) {
    if (obj instanceof CompositeKey) {
      CompositeKey other = (CompositeKey) obj;
      return other.getFirst().equals(getFirst())
          && other.getSecond().equals(getSecond());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getFirst().hashCode() ^ getSecond().hashCode();
  }

}
