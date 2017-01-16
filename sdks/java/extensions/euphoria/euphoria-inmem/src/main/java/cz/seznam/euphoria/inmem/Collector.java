package cz.seznam.euphoria.inmem;

interface Collector<T> {

  void collect(T elem);

}
