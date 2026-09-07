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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * This {@link DoFn} reads Cloud Spanner 'information_schema.*' tables to build the {@link
 * SpannerSchema}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ReadSpannerSchema extends DoFn<Void, SpannerSchema> {

  private final SpannerConfig config;

  private final PCollectionView<Dialect> dialectView;

  private final Set<String> allowedTableNames;

  private transient SpannerAccessor spannerAccessor;

  /**
   * Constructor for creating an instance of the ReadSpannerSchema class. If no {@code
   * allowedTableNames} is passed, every single table is allowed.
   *
   * @param config The SpannerConfig object that contains the configuration for accessing the
   *     Spanner database.
   * @param dialectView A PCollectionView object that holds a Dialect object for the database
   *     dialect to use for reading the Spanner schema.
   */
  public ReadSpannerSchema(SpannerConfig config, PCollectionView<Dialect> dialectView) {
    this(config, dialectView, new HashSet<String>());
  }

  /**
   * Constructor for creating an instance of the ReadSpannerSchema class.
   *
   * @param config The SpannerConfig object that contains the configuration for accessing the
   *     Spanner database.
   * @param dialectView A PCollectionView object that holds a Dialect object for the database
   *     dialect to use for reading the Spanner schema.
   * @param allowedTableNames A set of allowed table names to be used when reading the Spanner
   *     schema.
   */
  public ReadSpannerSchema(
      SpannerConfig config, PCollectionView<Dialect> dialectView, Set<String> allowedTableNames) {
    this.config = config;
    this.dialectView = dialectView;
    this.allowedTableNames = allowedTableNames == null ? new HashSet<>() : allowedTableNames;
  }

  /**
   * Reads Spanner schema information without running a Beam pipeline.
   *
   * <p>Used by SchemaTransforms during expansion (including cross-language expansion services that
   * do not ship DirectRunner).
   */
  public static SpannerSchema getSpannerSchema(
      SpannerConfig config, Dialect dialect, Set<String> allowedTableNames) {
    try (SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(config)) {
      return getSpannerSchema(spannerAccessor.getDatabaseClient(), dialect, allowedTableNames);
    }
  }

  static SpannerSchema getSpannerSchema(
      DatabaseClient databaseClient, Dialect dialect, Set<String> allowedTableNames) {
    Set<String> allowed = allowedTableNames == null ? Collections.emptySet() : allowedTableNames;
    SpannerSchema.Builder builder = SpannerSchema.builder(dialect);
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = readTableInfo(tx, dialect);

      while (resultSet.next()) {
        String tableName = resultSet.getString(0);
        String columnName = resultSet.getString(1);
        String type = resultSet.getString(2);
        long cellsMutated = resultSet.getLong(3);
        if (!isTableAllowed(allowed, tableName)) {
          continue;
        }
        builder.addColumn(tableName, columnName, type, cellsMutated);
      }

      resultSet = readPrimaryKeyInfo(tx, dialect);
      while (resultSet.next()) {
        String tableName = resultSet.getString(0);
        String columnName = resultSet.getString(1);
        String ordering = resultSet.getString(2);
        if (!isTableAllowed(allowed, tableName)) {
          continue;
        }
        builder.addKeyPart(tableName, columnName, "DESC".equalsIgnoreCase(ordering));
      }
    }
    return builder.build();
  }

  private static boolean isTableAllowed(Set<String> allowedTableNames, String tableName) {
    if (allowedTableNames.isEmpty()) {
      return true;
    }
    for (String allowed : allowedTableNames) {
      if (allowed.equalsIgnoreCase(tableName)) {
        return true;
      }
    }
    return false;
  }

  @Setup
  public void setup(PipelineOptions options) throws Exception {
    OpenTelemetry otel = options.as(SdkHarnessOptions.class).getOpenTelemetry();
    spannerAccessor = SpannerAccessor.getOrCreate(config, otel);
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    c.output(
        getSpannerSchema(
            spannerAccessor.getDatabaseClient(), c.sideInput(dialectView), allowedTableNames));
  }

  private static ResultSet readTableInfo(ReadOnlyTransaction tx, Dialect dialect) {
    // retrieve schema information for all tables, as well as aggregating the
    // number of indexes that cover each column. this will be used to estimate
    // the number of cells (table column plus indexes) mutated in an upsert operation
    // in order to stay below the 20k threshold
    String statement = "";
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            "SELECT"
                + "    c.table_name"
                + "  , c.column_name"
                + "  , c.spanner_type"
                + "  , (1 + COALESCE(t.indices, 0)) AS cells_mutated"
                + "  FROM ("
                + "    SELECT c.table_name, c.column_name, c.spanner_type, c.ordinal_position"
                + "     FROM information_schema.columns as c"
                + "     WHERE c.table_catalog = '' AND c.table_schema = '') AS c"
                + "  LEFT OUTER JOIN ("
                + "    SELECT t.table_name, t.column_name, COUNT(*) AS indices"
                + "      FROM information_schema.index_columns AS t "
                + "      WHERE t.index_name != 'PRIMARY_KEY' AND t.table_catalog = ''"
                + "      AND t.table_schema = ''"
                + "      GROUP BY t.table_name, t.column_name) AS t"
                + "  USING (table_name, column_name)"
                + "  ORDER BY c.table_name, c.ordinal_position";
        break;
      case POSTGRESQL:
        statement =
            "SELECT"
                + "    c.table_name"
                + "  , c.column_name"
                + "  , c.spanner_type"
                + "  , (1 + COALESCE(t.indices, 0)) AS cells_mutated"
                + "  FROM ("
                + "    SELECT c.table_name, c.column_name, c.spanner_type, c.ordinal_position"
                + "      FROM information_schema.columns as c"
                + "      WHERE c.table_schema='public') AS c"
                + "  LEFT OUTER JOIN ("
                + "    SELECT t.table_name, t.column_name, COUNT(*) AS indices"
                + "      FROM information_schema.index_columns AS t "
                + "      WHERE t.index_name != 'PRIMARY_KEY'"
                + "      AND t.table_schema='public'"
                + "      GROUP BY t.table_name, t.column_name) AS t"
                + "  USING (table_name, column_name)"
                + "  ORDER BY c.table_name, c.ordinal_position";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
    }
    return tx.executeQuery(Statement.of(statement));
  }

  private static ResultSet readPrimaryKeyInfo(ReadOnlyTransaction tx, Dialect dialect) {
    String statement = "";
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            "SELECT t.table_name, t.column_name, t.column_ordering"
                + " FROM information_schema.index_columns AS t "
                + " WHERE t.index_name = 'PRIMARY_KEY' AND t.table_catalog = ''"
                + " AND t.table_schema = ''"
                + " ORDER BY t.table_name, t.ordinal_position";
        break;
      case POSTGRESQL:
        statement =
            "SELECT t.table_name, t.column_name, t.column_ordering"
                + " FROM information_schema.index_columns AS t "
                + " WHERE t.index_name = 'PRIMARY_KEY'"
                + " AND t.table_schema='public'"
                + " ORDER BY t.table_name, t.ordinal_position";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
    }
    return tx.executeQuery(Statement.of(statement));
  }
}
