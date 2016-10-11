
package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Calculate maximum.
 */
public class Max {

  /** Return {@code CombinableReduceFunction} to calculate maximum of input. */
  public static <IN, X extends Comparable<X>> CombinableReduceFunction<IN> of(
      UnaryFunction<IN, X> extract) {
    
    return values -> {
      IN max = null;
      X maxValue = null;
      for (IN input : values) {
        X value = extract.apply(input);
        if (maxValue == null || maxValue.compareTo(value) < 0) {
          max = input;
          maxValue = value;
        }
      }
      return max;
    };
  }

  private Max() { }

}
