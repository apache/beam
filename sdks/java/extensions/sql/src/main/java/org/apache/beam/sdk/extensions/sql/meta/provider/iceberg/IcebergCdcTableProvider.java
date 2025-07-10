package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;

import java.util.Map;

public class IcebergCdcTableProvider extends IcebergTableProvider {

    public IcebergCdcTableProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public String getTableType() {
        return "iceberg_cdc";
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
        return new IcebergCdcTable(table, catalogConfig);
    }
}
