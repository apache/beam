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
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BigQueryTable} represent a BigQuery table as a target. This provider does not currently
 * support being a source.
 */
@Experimental
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class BigQueryTable extends SchemaBaseBeamTable implements Serializable {
  @VisibleForTesting static final String METHOD_PROPERTY = "method";
  @VisibleForTesting static final String WRITE_DISPOSITION_PROPERTY = "writeDisposition";
  @VisibleForTesting final String bqLocation;
  private final ConversionOptions conversionOptions;
  private BeamTableStatistics rowCountStatistics = null;
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTable.class);
  @VisibleForTesting final Method method;
  @VisibleForTesting final WriteDisposition writeDisposition;

  BigQueryTable(Table table, BigQueryUtils.ConversionOptions options) {
    super(table.getSchema());
    this.conversionOptions = options;
    this.bqLocation = table.getLocation();

    if (table.getProperties().containsKey(METHOD_PROPERTY)) {
      List<String> validMethods =
          Arrays.stream(Method.values()).map(Enum::toString).collect(Collectors.toList());
      // toUpperCase should make it case-insensitive
      String selectedMethod = table.getProperties().getString(METHOD_PROPERTY).toUpperCase();

      if (validMethods.contains(selectedMethod)) {
        method = Method.valueOf(selectedMethod);
      } else {
        throw new InvalidPropertyException(
            "Invalid method "
                + "'"
                + selectedMethod
                + "'. "
                + "Supported methods are: "
                + validMethods.toString()
                + ".");
      }
    } else {
      method = Method.DIRECT_READ;
    }

    LOG.info("BigQuery method is set to: " + method.toString());

    if (table.getProperties().containsKey(WRITE_DISPOSITION_PROPERTY)) {
      List<String> validWriteDispositions =
          Arrays.stream(WriteDisposition.values()).map(Enum::toString).collect(Collectors.toList());
      // toUpperCase should make it case-insensitive
      String selectedWriteDisposition =
          table.getProperties().getString(WRITE_DISPOSITION_PROPERTY).toUpperCase();

      if (validWriteDispositions.contains(selectedWriteDisposition)) {
        writeDisposition = WriteDisposition.valueOf(selectedWriteDisposition);
      } else {
        throw new InvalidPropertyException(
            "Invalid write disposition "
                + "'"
                + selectedWriteDisposition
                + "'. "
                + "Supported write dispositions are: "
                + validWriteDispositions.toString()
                + ".");
      }
    } else {
      writeDisposition = WriteDisposition.WRITE_EMPTY;
    }

    LOG.info("BigQuery writeDisposition is set to: " + writeDisposition.toString());
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {

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
    return begin.apply("Read Input BQ Rows", getBigQueryTypedRead(getSchema()));
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    if (!method.equals(Method.DIRECT_READ)) {
      LOG.info("Predicate/project push-down only available for `DIRECT_READ` method, skipping.");
      return buildIOReader(begin);
    }

    final FieldAccessDescriptor resolved =
        FieldAccessDescriptor.withFieldNames(fieldNames).resolve(getSchema());
    final Schema newSchema = SelectHelpers.getOutputSchema(getSchema(), resolved);

    TypedRead<Row> typedRead = getBigQueryTypedRead(newSchema);

    if (!(filters instanceof DefaultTableFilter)) {
      BigQueryFilter bigQueryFilter = (BigQueryFilter) filters;
      if (!bigQueryFilter.getSupported().isEmpty()) {
        String rowRestriction = generateRowRestrictions(getSchema(), bigQueryFilter.getSupported());
        if (!rowRestriction.isEmpty()) {
          LOG.info("Pushing down the following filter: " + rowRestriction);
          typedRead = typedRead.withRowRestriction(rowRestriction);
        }
      }
    }

    if (!fieldNames.isEmpty()) {
      typedRead = typedRead.withSelectedFields(fieldNames);
    }

    return begin.apply("Read Input BQ Rows with push-down", typedRead);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input.apply(
        BigQueryIO.<Row>write()
            .withSchema(BigQueryUtils.toTableSchema(getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .withWriteDisposition(writeDisposition)
            .to(bqLocation));
  }

  @Override
  public ProjectSupport supportsProjects() {
    return method.equals(Method.DIRECT_READ)
        ? ProjectSupport.WITHOUT_FIELD_REORDERING
        : ProjectSupport.NONE;
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    if (method.equals(Method.DIRECT_READ)) {
      return new BigQueryFilter(filter);
    }

    return super.constructFilter(filter);
  }

  private String generateRowRestrictions(Schema schema, List<RexNode> supported) {
    assert !supported.isEmpty();
    final IntFunction<SqlNode> field =
        i -> new SqlIdentifier(schema.getField(i).getName(), SqlParserPos.ZERO);

    // TODO: BigQuerySqlDialectWithTypeTranslation can be replaced with BigQuerySqlDialect after
    // updating vendor Calcite version.
    SqlImplementor.Context context = new BeamSqlUnparseContext(field);

    // Create a single SqlNode from a list of RexNodes
    SqlNode andSqlNode = null;
    for (RexNode node : supported) {
      SqlNode sqlNode = context.toSql(null, node);
      if (andSqlNode == null) {
        andSqlNode = sqlNode;
        continue;
      }
      // AND operator must have exactly 2 operands.
      andSqlNode =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO, ImmutableList.of(andSqlNode, sqlNode));
    }

    return andSqlNode.toSqlString(BeamBigQuerySqlDialect.DEFAULT).getSql();
  }

  private TypedRead<Row> getBigQueryTypedRead(Schema schema) {
    return BigQueryIO.read(
            record -> BigQueryUtils.toBeamRow(record.getRecord(), schema, conversionOptions))
        .withMethod(method)
        .from(bqLocation)
        .withCoder(SchemaCoder.of(schema));
  }

  private static BeamTableStatistics getRowCountFromBQ(PipelineOptions o, String bqLocation) {
    try {
      BigInteger rowCount =
          BigQueryHelpers.getNumRows(
              o.as(BigQueryOptions.class), BigQueryHelpers.parseTableSpec(bqLocation));

      if (rowCount == null) {
        return BeamTableStatistics.BOUNDED_UNKNOWN;
      }

      return BeamTableStatistics.createBoundedTableStatistics(rowCount.doubleValue());

    } catch (IOException | InterruptedException e) {
      LOG.warn("Could not get the row count for the table " + bqLocation, e);
    }

    return BeamTableStatistics.BOUNDED_UNKNOWN;
  }

  public static class InvalidPropertyException extends UnsupportedOperationException {
    private InvalidPropertyException(String s) {
      super(s);
    }
  }
}
