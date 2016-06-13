package cz.seznam.euphoria.fluent;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.util.Settings;

import static java.util.Objects.requireNonNull;

public class Flow {
  public static Flow create(String name) {
    return new Flow(cz.seznam.euphoria.core.client.flow.Flow.create(name));
  }

  public static Flow create(String name, Settings settings) {
    return new Flow(cz.seznam.euphoria.core.client.flow.Flow.create(name, settings));
  }

  private final cz.seznam.euphoria.core.client.flow.Flow wrap;

  Flow(cz.seznam.euphoria.core.client.flow.Flow wrap) {
    this.wrap = requireNonNull(wrap);
  }

  public <T> Dataset<T> read(DataSource<T> src) {
    return new Dataset<>(wrap.createInput(src));
  }
}
