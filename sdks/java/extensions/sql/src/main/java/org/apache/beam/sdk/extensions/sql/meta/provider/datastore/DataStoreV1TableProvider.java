package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

@AutoService(TableProvider.class)
public class DataStoreV1TableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "datastoreV1";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new DataStoreV1Table(table);
  }
}
