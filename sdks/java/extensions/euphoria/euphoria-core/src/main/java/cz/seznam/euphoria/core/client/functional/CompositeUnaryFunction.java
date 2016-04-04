
package cz.seznam.euphoria.core.client.functional;

/**
 * A function that is composition of two unary functions.
 */
public class CompositeUnaryFunction<IN, OUT, X> implements UnaryFunction<IN, OUT> {

  private final UnaryFunction<IN, X> first;
  private final UnaryFunction<X, OUT> second;

  public static <IN, OUT, X> CompositeUnaryFunction<IN, OUT, X> of(
      UnaryFunction<IN, X> first, UnaryFunction<X, OUT> second) {
    return new CompositeUnaryFunction<>(first, second);
  }

  private CompositeUnaryFunction(
      UnaryFunction<IN, X> first, UnaryFunction<X, OUT> second) {
    this.first = first;
    this.second = second;
  }

  @Override
  @SuppressWarnings("unchecked")
  public OUT apply(IN what) {
    return second.apply(first.apply(what));
  }

}
