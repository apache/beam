package cz.seznam.euphoria.core.client.util;

import java.util.Comparator;
import java.util.Objects;

/**
 * Pair of any types. The pair has to fit in memory and is read-only.
 */
public class Pair<K, V> implements java.util.Map.Entry<K, V> {

  private static final Comparator<Pair> CMP_BY_FIRST =
          (o1, o2) -> doCompare(o1.getFirst(), o2.getFirst());

  private static final Comparator<Pair> CMP_BY_SECOND =
          (o1, o2) -> doCompare(o1.getSecond(), o2.getSecond());

  @SuppressWarnings("unchecked")
  private static int doCompare(Object a, Object b) {
    Comparable ca = (Comparable) a;
    Comparable cb = (Comparable) b;
    // ~ ensure nulls are produced last
    if (ca == cb) { return 0; }
    if (ca == null) { return 1; }
    if (cb == null) { return -1; }
    return ca.compareTo(cb);
  }

  /**
   * Retrieves a comparator which compares pairs by their {@link #getFirst()} member.
   */
  @SuppressWarnings({"unchecked", "UnusedDeclaration"})
  public static <K extends Comparable<K>, V> Comparator<Pair<K, V>> compareByFirst() {
    return (Comparator) CMP_BY_FIRST;
  }

  /**
   * Retrieves a comparator which compares pairs by their {@link #getSecond()} member.
   */
  @SuppressWarnings({"unchecked", "UnusedDeclaration"})
  public static <K, V extends Comparable<V>> Comparator<Pair<K, V>> compareBySecond() {
    return (Comparator) CMP_BY_SECOND;
  }

  // ~ -----------------------------------------------------------------------------

  final K first;
  final V second;

  protected Pair(K first, V second) {
    this.first = first;
    this.second = second;
  }

  /**
   * Construct the pair.
   */
  public static <K, V> Pair<K, V> of(K first, V second) {
    return new Pair<>(first, second);
  }

  public K getFirst() {
    return first;
  }

  public V getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return String.format("Pair{first='%s', second='%s'}",
        Objects.toString(first), Objects.toString(second));
  }

  // ~ Map.Entry implementation -----------------------------------------------------

  @Override
  public K getKey() { return first; }

  @Override
  public V getValue() { return second; }

  /** Always throws {@link java.lang.UnsupportedOperationException}. */
  @Override
  public V setValue(V value) {
    throw new UnsupportedOperationException("Read-only entry!");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Pair) {
      Pair other = (Pair) obj;
      return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first) ^ Objects.hashCode(second);
  }


}
