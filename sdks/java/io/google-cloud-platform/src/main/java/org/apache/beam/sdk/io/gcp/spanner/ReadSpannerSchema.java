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
import java.util.HashSet;
import java.util.Set;
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
   * Constructor for creating an instance of the ReadSpannerSchema class. If no {@param
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

  @Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(config);
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    Dialect dialect = c.sideInput(dialectView);
    SpannerSchema.Builder builder = SpannerSchema.builder(dialect);
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      ResultSet resultSet = readTableInfo(tx, dialect);

      while (resultSet.next()) {
        String schemaName = resultSet.getString(0); // TABLE_SCHEMA
        String tableName = resultSet.getString(1);  // TABLE_NAME
        String fullTableName = schemaName.isEmpty() ? tableName : schemaName + "." + tableName;
        String columnName = resultSet.getString(2); // COLUMN_NAME
        String type = resultSet.getString(3);       // SPANNER_TYPE
        long cellsMutated = resultSet.getLong(4);   // CELLS_MUTATED

        // Apply allowedTableNames filter on full table name if specified
        if (allowedTableNames.size() > 0 && !allowedTableNames.contains(fullTableName)) {
          continue;
        }
        builder.addColumn(fullTableName, columnName, type, cellsMutated);
      }

      resultSet = readPrimaryKeyInfo(tx, dialect);
      while (resultSet.next()) {
        String schemaName = resultSet.getString(0); // TABLE_SCHEMA
        String tableName = resultSet.getString(1);  // TABLE_NAME
        String fullTableName = schemaName.isEmpty() ? tableName : schemaName + "." + tableName;
        String columnName = resultSet.getString(2); // COLUMN_NAME
        String ordering = resultSet.getString(3);   // COLUMN_ORDERING

        // Apply allowedTableNames filter on full table name if specified
        if (allowedTableNames.size() > 0 && !allowedTableNames.contains(fullTableName)) {
          continue;
        }
        builder.addKeyPart(fullTableName, columnName, "DESC".equalsIgnoreCase(ordering));
      }
    }
    c.output(builder.build());
  }

  private ResultSet readTableInfo(ReadOnlyTransaction tx, Dialect dialect) {
    // Retrieve schema information for all tables across all schemas, including the number of
    // indexes covering each column to estimate cells mutated in upserts.
    String statement = "";
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            "SELECT "
                + "  c.TABLE_SCHEMA, "
                + "  c.TABLE_NAME, "
                + "  c.COLUMN_NAME, "
                + "  c.SPANNER_TYPE, "
                + "  (1 + COALESCE(t.indices, 0)) AS cells_mutated "
                + "FROM INFORMATION_SCHEMA.COLUMNS AS c "
                + "LEFT OUTER JOIN ("
                + "  SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME, COUNT(*) AS indices "
                + "  FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS t "
                + "  WHERE t.INDEX_NAME != 'PRIMARY_KEY' AND t.TABLE_CATALOG = '' "
                + "  GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME "
                + ") AS t "
                + "ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME AND c.COLUMN_NAME = t.COLUMN_NAME "
                + "WHERE c.TABLE_CATALOG = '' "
                + "ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION";
        break;
      case POSTGRESQL:
        statement =
            "SELECT "
                + "  c.TABLE_SCHEMA, "
                + "  c.TABLE_NAME, "
                + "  c.COLUMN_NAME, "
                + "  c.SPANNER_TYPE, "
                + "  (1 + COALESCE(t.indices, 0)) AS cells_mutated "
                + "FROM INFORMATION_SCHEMA.COLUMNS AS c "
                + "LEFT OUTER JOIN ("
                + "  SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME, COUNT(*) AS indices "
                + "  FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS t "
                + "  WHERE t.INDEX_NAME != 'PRIMARY_KEY' "
                + "  GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.COLUMN_NAME "
                + ") AS t "
                + "ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME AND c.COLUMN_NAME = t.COLUMN_NAME "
                + "ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
    }
    return tx.executeQuery(Statement.of(statement));
  }

  private ResultSet readPrimaryKeyInfo(ReadOnlyTransaction tx, Dialect dialect) {
    String statement = "";
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            "SELECT "
                + "  t.TABLE_SCHEMA, "
                + "  t.TABLE_NAME, "
                + "  t.COLUMN_NAME, "
                + "  t.COLUMN_ORDERING "
                + "FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS t "
                + "WHERE t.INDEX_NAME = 'PRIMARY_KEY' AND t.TABLE_CATALOG = '' "
                + "ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, t.ORDINAL_POSITION";
        break;
      case POSTGRESQL:
        statement =
            "SELECT "
                + "  t.TABLE_SCHEMA, "
                + "  t.TABLE_NAME, "
                + "  t.COLUMN_NAME, "
                + "  t.COLUMN_ORDERING "
                + "FROM INFORMATION_SCHEMA.INDEX_COLUMNS AS t "
                + "WHERE t.INDEX_NAME = 'PRIMARY_KEY' "
                + "ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, t.ORDINAL_POSITION";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
    }
    return tx.executeQuery(Statement.of(statement));
  }
}