package cz.seznam.euphoria.fluent;

import cz.seznam.euphoria.core.util.Settings;

/** Helper class providing convenient start points into the fluent api. */
public class Fluent {

  public static Flow flow(String name) {
    return Flow.create(name);
  }

  public static Flow flow(String name, Settings settings) {
    return Flow.create(name, settings);
  }

  public static <T> Dataset<T>
  lift(cz.seznam.euphoria.core.client.dataset.Dataset<T> xs) {
    return new Dataset<>(xs);
  }

}
