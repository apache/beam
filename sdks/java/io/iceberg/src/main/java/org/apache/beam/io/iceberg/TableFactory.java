package org.apache.beam.io.iceberg;

import java.io.Serializable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

@SuppressWarnings("all")
public abstract class TableFactory<IdentifierT> implements Serializable {

  private TableFactory() { }

  public abstract Table getTable(IdentifierT id);

  public static TableFactory<String> forCatalog(final Iceberg.Catalog catalog) {
    return new TableFactory<String>() {
      @Override
      public Table getTable(String id) {
        //Hack to remove the name of the catalog.
        id = id.substring(id.indexOf('.')+1);
        return catalog.catalog().loadTable(TableIdentifier.parse(id));
      }
    };
  }


}
