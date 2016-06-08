package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;

public interface OutputProvider<T> {

  Dataset<T> output();

}
