package cz.seznam.euphoria.flink.streaming;

public interface ElementProvider<T> {

  T getElement();

}
