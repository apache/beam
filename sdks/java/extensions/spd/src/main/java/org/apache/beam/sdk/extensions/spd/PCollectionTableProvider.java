package org.apache.beam.sdk.extensions.spd;

import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

import java.util.Map;

public class PCollectionTableProvider implements TableProvider {

    private String tableType;
    public PCollectionTableProvider(String tableType) {
        this.tableType = tableType;
    }


    @Override
    public String getTableType() {
        return tableType;
    }

    public void createTableFromPCollection()

    @Override
    public void createTable(Table table) {

    }

    @Override
    public void dropTable(String tableName) {

    }

    @Override
    public Map<String, Table> getTables() {
        return null;
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
        return null;
    }
}
