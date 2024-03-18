package org.apache.beam.sdk.io.catalog;

public interface CatalogEnvironment {

  String defaultNamespace();

  CatalogResource find(CatalogResourceIdentifier id);
  default CatalogResource find(String...path) {
    return find(new CatalogResourceIdentifier(path));
  }

}
