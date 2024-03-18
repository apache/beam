package org.apache.beam.sdk.io.catalog;

import java.util.Arrays;

public class CatalogResourceIdentifier {
  private String[] namespace;
  private String name;

  public CatalogResourceIdentifier(String...name) {
    if(name.length == 1) {
      this.name = name[0];
      this.namespace = new String[0];
    } else {
      this.name = name[name.length-1];
      this.namespace = Arrays.copyOf(name,name.length-1);
    }
  }

  public static CatalogResourceIdentifier of(String...name) {
    return new CatalogResourceIdentifier(name);
  }

}
