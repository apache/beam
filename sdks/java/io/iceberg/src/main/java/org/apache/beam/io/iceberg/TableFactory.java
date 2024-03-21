package org.apache.beam.io.iceberg;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@SuppressWarnings("all")
public abstract class TableFactory<IdentifierT> implements Serializable {

  private TableFactory() { }

  public abstract Table getTable(IdentifierT id);

  public static TableFactory<String> forCatalog(final Iceberg.Catalog catalog) {
    return new TableFactory<String>() {
      @Override
      public Table getTable(String id) {
        TableIdentifier tableId = TableIdentifier.parse(id);
        //If the first element in the namespace is our catalog, remove that.
        if(tableId.hasNamespace()) {
          Namespace ns = tableId.namespace();
          if(catalog.catalog().name().equals(ns.level(0))) {
            String[] levels = ns.levels();
            levels = Arrays.copyOfRange(levels,1,levels.length);
            tableId = TableIdentifier.of(
                Namespace.of(levels),
                tableId.name());
          }
        }
        return catalog.catalog().loadTable(tableId);
      }
    };
  }


}
