package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.util.Pair;

/**
 * {@link Pair} implementing {@link Comparable} interface to be used
 * in sort-based shuffle in Flink executor.
 */
public class ComparablePair<T0, T1>
        extends Pair<T0, T1>
        implements Comparable<ComparablePair<T0, T1>> {
  
  ComparablePair(T0 first, T1 second) {
    super(first, second);
  }

  public static <T0, T1> ComparablePair<T0, T1> of(T0 first, T1 second) {
    return new ComparablePair<>(first, second);
  }

  @Override
  public int compareTo(ComparablePair<T0, T1> o) {
    int result = compare(getFirst(), o.getFirst());
    if (result == 0) {
      result = compare(getSecond(), o.getSecond());
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  private int compare(Object obj1, Object obj2) {
    if (obj1 instanceof Comparable && obj2 instanceof Comparable) {
      return ((Comparable) obj1).compareTo(obj2);
    }
    final Object wrong = obj1 instanceof Comparable ? obj2 : obj1;
    throw new IllegalArgumentException("Got class " + wrong.getClass()
        + " which is not Comparable! Please pass two Comparables for now!");
  }
}
