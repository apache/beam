package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;

/** Provides commonly used function objects around computing sums. */
public class Sums {

  private static final CombinableReduceFunction<Long> SUMS_OF_LONG =
      (CombinableReduceFunction<Long>) xs -> {
        long ret = 0L;
        for (Long x : xs) {
          ret += x;
        }
        return ret;
      };

  public static CombinableReduceFunction<Long> ofLongs() {
    return SUMS_OF_LONG;
  }

  private Sums() {}
}
