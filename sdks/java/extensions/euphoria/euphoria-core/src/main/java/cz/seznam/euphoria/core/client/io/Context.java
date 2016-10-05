
package cz.seznam.euphoria.core.client.io;

/**
 * A collector of elements. Used in functors.
 */
public interface Context<T> {

  /**
   * Collects the given element to the output of this context.
   */
  void collect(T elem);

}