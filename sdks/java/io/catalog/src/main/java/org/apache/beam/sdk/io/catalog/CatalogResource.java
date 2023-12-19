package org.apache.beam.sdk.io.catalog;

/**
 * Generic interface for catalog resources.
 */
public interface CatalogResource {

  /**
   *
   * @return Whether or not you can use this resource as a source
   */
  default boolean isSource() {
    return false;
  }

  /**
   *
   * @return Whether or not you can use this resource as a sink
   */
  default boolean isSink() {
    return false;
  }

  /**
   *
   * @return Whether or not you can use this resource as a function/transform.
   */
  default boolean isTransform() {
    return false;
  }

}
