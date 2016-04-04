
package cz.seznam.euphoria.core.client.io;

/**
 * A collector of elements. Used in functors.
 */
public interface Collector<T> {

  void collect(T elem);

}
