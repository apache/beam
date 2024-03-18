package org.apache.beam.sdk.io.catalog;

public interface CatalogSinkResource extends CatalogResource {

  @Override
  default boolean isSink() {
    return true;
  }
}
