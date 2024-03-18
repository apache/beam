package org.apache.beam.sdk.io.catalog;

public interface CatalogSourceResource extends CatalogResource {

  @Override
  default boolean isSource() {
    return true;
  }
}
