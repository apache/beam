package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BigQueryPerfTable extends BigQueryTable {
  private final String namespace;
  private final String metric;

  BigQueryPerfTable(Table table, ConversionOptions options, String namespace, String metric) {
    super(table, options);
    this.namespace = namespace;
    this.metric = metric;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return super.buildIOReader(begin).apply(ParDo.of(new TimeMonitor<>(namespace, metric)));
  }
}
