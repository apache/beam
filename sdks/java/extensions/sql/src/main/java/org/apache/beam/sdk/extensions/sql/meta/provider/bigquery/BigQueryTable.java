/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BigQueryTable} represent a BigQuery table as a target. This provider does not currently
 * support being a source.
 */
@Experimental
class BigQueryTable extends BaseBeamTable implements Serializable {
  @VisibleForTesting final String bqLocation;
  private final ConversionOptions conversionOptions;
  private BeamTableStatistics rowCountStatistics = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryTable.class);

  BigQueryTable(Table table, BigQueryUtils.ConversionOptions options) {
    super(table.getSchema());
    this.conversionOptions = options;
    this.bqLocation = table.getLocation();
  }

  @Override
  public BeamTableStatistics getRowCount(PipelineOptions options) {

    if (rowCountStatistics == null) {
      rowCountStatistics = getRowCountFromBQ(options, bqLocation);
    }

    return rowCountStatistics;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(
            "Read Input BQ Rows",
            BigQueryIO.read(
                    record ->
                        BigQueryUtils.toBeamRow(record.getRecord(), getSchema(), conversionOptions))
                .from(bqLocation)
                .withCoder(SchemaCoder.of(getSchema())))
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input.apply(
        BigQueryIO.<Row>write()
            .withSchema(BigQueryUtils.toTableSchema(getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .to(bqLocation));
  }

  private static BeamTableStatistics getRowCountFromBQ(PipelineOptions o, String bqLocation) {
    try {
      BigInteger rowCount =
          BigQueryHelpers.getNumRows(
              o.as(BigQueryOptions.class), BigQueryHelpers.parseTableSpec(bqLocation));

      if (rowCount == null) {
        return BeamTableStatistics.UNKNOWN;
      }

      return BeamTableStatistics.createBoundedTableStatistics(rowCount.doubleValue());

    } catch (IOException | InterruptedException e) {
      LOGGER.warn("Could not get the row count for the table " + bqLocation, e);
    }

    return BeamTableStatistics.UNKNOWN;
  }
}
