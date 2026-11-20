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
package org.apache.beam.sdk.extensions.sql.expansion;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@AutoService(SchemaTransformProvider.class)
public class SqlTransformSchemaTransformProvider implements SchemaTransformProvider {

  private static final Map<String, Class<? extends QueryPlanner>> QUERY_PLANNERS =
      ImmutableMap.of("calcite", org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.class);

  @Override
  public String identifier() {
    return "schematransform:org.apache.beam:sql_transform:v1";
  }

  @Override
  public String description() {
    return "A transform that executes a SQL query on its input PCollections.\n\n"
        + "If a single input is given, it may be referred to as `PCOLLECTION`, e.g. the query could be of the form"
        + "\n\n"
        + "    SELECT a, sum(b) FROM PCOLLECTION"
        + "\n\n"
        + "If multiple inputs are given, the should be named as they are in the query, e.g."
        + "\n\n"
        + "    SELECT a, b, c FROM pcoll_1 join pcoll_2 using (b)"
        + "\n\n"
        + "For more details about Beam SQL in general see "
        + "[the Beam SQL documentation](https://beam.apache.org/documentation/dsls/sql/overview/).";
  }

  /**
   * The configuration schema deliberately uses only primitive field types (strings, booleans and
   * arrays of strings) so that it can be decoded by every SDK. Logical types such as {@code
   * EnumerationType} and {@code OneOfType} are Java-specific and cannot be decoded from Python.
   *
   * <p>{@code dialect} must be one of the supported query planner names and each entry of {@code
   * tableproviders} must be the table type of an available {@link TableProvider}; both are
   * validated in {@link #from(Row)}.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.of(
        Schema.Field.of("query", Schema.FieldType.STRING).withDescription("SQL query to execute"),
        Schema.Field.nullable(
            "ddl", Schema.FieldType.STRING), // TODO: Underlying builder seems more capable?
        Schema.Field.nullable("dialect", Schema.FieldType.STRING)
            .withDescription(
                "Query planner to use. Supported values: "
                    + String.join(", ", QUERY_PLANNERS.keySet())),
        Schema.Field.nullable("autoload", Schema.FieldType.BOOLEAN),
        Schema.Field.nullable("tableproviders", Schema.FieldType.array(Schema.FieldType.STRING))
            .withDescription("Names (table types) of the table providers to make available."));
  }

  private static Map<String, TableProvider> availableTableProviders() {
    Map<String, TableProvider> tableProviders = new HashMap<>();
    ServiceLoader.load(TableProvider.class)
        .forEach((provider) -> tableProviders.put(provider.getTableType(), provider));
    return tableProviders;
  }

  @Override
  public SchemaTransform from(Row configuration) {
    String dialect = configuration.getString("dialect");
    if (dialect != null && !QUERY_PLANNERS.containsKey(dialect)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported dialect '%s'. Supported dialects: %s.",
              dialect, QUERY_PLANNERS.keySet()));
    }
    Collection<String> requested = configuration.getArray("tableproviders");
    if (requested != null) {
      Set<String> available = availableTableProviders().keySet();
      for (String name : requested) {
        if (name != null && !available.contains(name)) {
          throw new IllegalArgumentException(
              String.format(
                  "Unknown table provider '%s'. Available table providers: %s.",
                  name, new TreeSet<>(available)));
        }
      }
    }
    return new SqlSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return ImmutableList.of("output", "errors");
  }

  static class ErrorCapture extends PTransform<PCollection<Row>, PDone> {

    transient List<PCollection<Row>> inputs = new ArrayList<>();

    public List<PCollection<Row>> getInputs() {
      return inputs;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
      input.apply(
          "noop_" + inputs.size(),
          MapElements.into(TypeDescriptors.nulls())
              .via(
                  err -> {
                    return null;
                  }));
      inputs.add(input);
      return PDone.in(input.getPipeline());
    }
  }

  static class SqlSchemaTransform extends SchemaTransform {
    final Row config;

    public SqlSchemaTransform(Row config) {
      this.config = config;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

      // Start with the query. In theory the exception can't be thrown, but all this nullness
      // stuff
      // isn't actually smart enough to know that. Could just cop and suppress that warning, but
      // doing it the hard way for some reason.
      String queryString = config.getString("query");
      if (queryString == null) {
        throw new IllegalArgumentException("Configuration must provide a query string.");
      }
      SqlTransform transform = SqlTransform.query(queryString);

      // Allow setting the query planner class via the dialect name.
      String dialect = config.getString("dialect");
      if (dialect != null) {
        Class<? extends QueryPlanner> queryPlannerClass = QUERY_PLANNERS.get(dialect);
        if (queryPlannerClass != null) {
          transform = transform.withQueryPlannerClass(queryPlannerClass);
        }
      }

      // Add any DDL strings
      String ddl = config.getString("ddl");
      if (ddl != null) {
        transform = transform.withDdlString(ddl);
      }

      // Check to see if we autoload or not
      Boolean autoload = config.getBoolean("autoload");
      if (autoload != null && autoload) {
        transform = transform.withAutoLoading(true);
      } else {
        transform = transform.withAutoLoading(false);

        // Add any user specified table providers from the set of available tableproviders.
        Map<String, TableProvider> tableProviders = availableTableProviders();
        Collection<String> tableproviderList = config.getArray("tableproviders");
        if (tableproviderList != null) {
          for (String name : tableproviderList) {
            TableProvider p = name == null ? null : tableProviders.get(name);
            if (p != null) { // Names are validated in from(Row).
              transform = transform.withTableProvider(p.getTableType(), p);
            }
          }
        }
      }

      // TODO: Support query parameters (positional/named) using primitive field types.

      // TODO: See about reimplementing a correct version of SqlTransform
      ErrorCapture errors = new ErrorCapture();
      PCollection<Row> output = input.apply(transform.withErrorsTransformer(errors));

      // TODO: One possibility for capturing the required tables would be to inject a
      // tableprovider
      // that we control and see which tables are requested during expansion. We could then
      // modify the output schema to reflect these inputs via options for better validation.

      List<PCollection<Row>> errorList = errors.getInputs();
      if (errorList.size() == 0) {
        PCollection<Row> emptyErrors =
            input.getPipeline().apply(Create.empty(BeamSqlRelUtils.getErrorRowSchema(Schema.of())));
        return PCollectionRowTuple.of("output", output, "errors", emptyErrors);
      } else if (errorList.size() == 1) {
        return PCollectionRowTuple.of("output", output, "errors", errorList.get(0));
      } else {
        throw new UnsupportedOperationException(
            "SqlTransform currently only supports a single dead letter queue collection");
      }
    }
  }
}
