
package cz.seznam.euphoria.core.client.io;

/**
 * A collector of elements. Used in functors.
 */
@FunctionalInterface
public interface Collector<T> {

  void collect(T elem);

}
