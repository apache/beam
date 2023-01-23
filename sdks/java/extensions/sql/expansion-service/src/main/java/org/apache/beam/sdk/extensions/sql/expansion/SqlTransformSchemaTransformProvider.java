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

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.auto.service.AutoService;
import java.util.*;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class SqlTransformSchemaTransformProvider implements SchemaTransformProvider {

  private static final OneOfType QUERY_PARAMETER =
      OneOfType.create(
          Schema.Field.nullable("string", Schema.FieldType.STRING),
          Schema.Field.nullable("short", Schema.FieldType.INT16),
          Schema.Field.nullable("int", Schema.FieldType.INT32),
          Schema.Field.nullable("long", Schema.FieldType.INT64),
          Schema.Field.nullable("float", Schema.FieldType.FLOAT),
          Schema.Field.nullable("double", Schema.FieldType.DOUBLE));

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "schematransform:org.apache.beam:sql_transform:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Schema configurationSchema() {
    return Schema.of(
        Schema.Field.of("query", Schema.FieldType.STRING),
        Schema.Field.nullable("dialect", Schema.FieldType.STRING),
        Schema.Field.nullable("autoload", Schema.FieldType.BOOLEAN),
        Schema.Field.nullable("tableproviders", Schema.FieldType.array(Schema.FieldType.STRING)),
        Schema.Field.nullable(
            "parameters",
            Schema.FieldType.logicalType(
                OneOfType.create(
                    Schema.Field.of(
                        "positional",
                        Schema.FieldType.array(Schema.FieldType.logicalType(QUERY_PARAMETER))),
                    Schema.Field.of(
                        "named",
                        Schema.FieldType.array(Schema.FieldType.logicalType(QUERY_PARAMETER)))))));
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      @UnknownKeyFor @NonNull @Initialized Row configuration) {
    return new SqlSchemaTransform(configuration);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return ImmutableList.of("output", "errors");
  }

  static class SqlSchemaTransform implements SchemaTransform {
    final Row config;

    public SqlSchemaTransform(Row config) {
      this.config = config;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PTransform<
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
        buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
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

          String dialect = config.getString("dialect");
          // We default to the Calcite implementation as it has better support (for some value of
          // better)
          if (dialect == null) {
            dialect = "calcite";
          }

          // Check to see if we autoload or not
          Boolean autoload = config.getBoolean("autoload");
          if (autoload != null && autoload) {
            transform = transform.withAutoLoading(true);
          } else {
            transform = transform.withAutoLoading(false);

            // Add any user specified table providers
            Map<String, TableProvider> tableProviders = new HashMap<>();
            ServiceLoader.load(TableProvider.class)
                .forEach(
                    (provider) -> {
                      tableProviders.put(provider.getTableType(), provider);
                    });
            Collection<?> tableproviderList = config.getArray("tableproviders");
            if (tableproviderList != null) {
              for (Object nameObj : tableproviderList) {
                if (nameObj != null) { // This actually could in theory be null...
                  TableProvider p = tableProviders.get(nameObj);
                  if (p != null) {
                    transform = transform.withTableProvider(p.getTableType(), p);
                  }
                }
              }
            }
          }

          return PCollectionRowTuple.of("output", input.apply(transform));
        }
      };
    }
  }
}
