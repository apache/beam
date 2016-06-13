package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;

public interface OutputBuilder<T> {

  Dataset<T> output();

}
