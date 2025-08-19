package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergCdcTable extends IcebergTable {
    static final String STREAMING_FIELD = "streaming";
    private final boolean isStreaming;
    IcebergCdcTable(Table table, IcebergCatalogConfig catalogConfig) {
        super(table, catalogConfig);
        this.isStreaming = table.getProperties().has(STREAMING_FIELD) && table.getProperties().get(STREAMING_FIELD).asBoolean();
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
        throw new UnsupportedOperationException("Writing CDC to Iceberg is not supported yet.");
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
        return begin
            .apply("Read Iceberg CDC", Managed.read(Managed.ICEBERG_CDC).withConfig(getBaseConfig()))
            .getSinglePCollection();
    }

    @Override
    public PCollection<Row> buildIOReader(
        PBegin begin, BeamSqlTableFilter filter, List<String> fieldNames) {

        Map<String, Object> readConfig = getPushdownConfig(filter, fieldNames);

        return begin
            .apply("Read Iceberg CDC with push-down", Managed.read(Managed.ICEBERG_CDC).withConfig(readConfig))
            .getSinglePCollection();
    }

    @Override
    public PCollection.IsBounded isBounded() {
        return isStreaming
            ? PCollection.IsBounded.UNBOUNDED
            : PCollection.IsBounded.BOUNDED;
    };
}
