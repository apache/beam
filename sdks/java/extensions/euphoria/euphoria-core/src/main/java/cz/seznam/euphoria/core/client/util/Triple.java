package cz.seznam.euphoria.core.client.util;

import java.util.Objects;

/**
 * Triple of any types.
 */
public class Triple<F, S, T> {
  final F first;
  final S second;
  final T third;

  protected Triple(F first, S second, T third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public static <F, S, T> Triple<F, S, T>of(F first, S second, T third) {
    return new Triple<>(first, second, third);
  }

  public F getFirst() {
    return first;
  }

  public S getSecond() {
    return second;
  }

  public T getThird() {
    return third;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Triple) {
      Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
      return Objects.equals(first, triple.first) &&
          Objects.equals(second, triple.second) &&
          Objects.equals(third, triple.third);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third);
  }

  @Override
  public String toString() {
    return "Triple{" +
        "first=" + first +
        ", second=" + second +
        ", third=" + third +
        '}';
  }
}
