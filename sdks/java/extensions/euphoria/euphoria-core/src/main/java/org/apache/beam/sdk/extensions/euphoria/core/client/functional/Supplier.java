package org.apache.beam.sdk.extensions.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Represents a serializable supplier of some object.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #get()}.
 *
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface Supplier<T> extends Serializable {

  /**
   * Gets a supplied object.
   *
   * @return a result
   */
  T get();
}
