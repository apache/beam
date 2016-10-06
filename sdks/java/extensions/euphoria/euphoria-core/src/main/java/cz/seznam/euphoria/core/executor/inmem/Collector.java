package cz.seznam.euphoria.core.executor.inmem;

interface Collector<T> {

  void collect(T elem);

}
