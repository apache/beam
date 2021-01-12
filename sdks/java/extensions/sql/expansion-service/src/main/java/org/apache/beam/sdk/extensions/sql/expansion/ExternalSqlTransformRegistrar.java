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
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(ExternalTransformRegistrar.class)
public class ExternalSqlTransformRegistrar implements ExternalTransformRegistrar {
  private static final String URN = "beam:external:java:sql:v1";
  private static final ImmutableMap<String, Class<? extends QueryPlanner>> DIALECTS =
      ImmutableMap.<String, Class<? extends QueryPlanner>>builder()
          .put("zetasql", ZetaSQLQueryPlanner.class)
          .put("calcite", CalciteQueryPlanner.class)
          .build();

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap.of(
        URN, Builder.class);
  }

  public static class Configuration {
    String query = "";
    @Nullable String dialect;

    public void setQuery(String query) {
      this.query = query;
    }

    public void setDialect(@Nullable String dialect) {
      this.dialect = dialect;
    }
  }

  private static class Builder
      implements ExternalTransformBuilder<Configuration, PInput, PCollection<Row>> {
    @Override
    public PTransform<PInput, PCollection<Row>> buildExternal(Configuration configuration) {
      SqlTransform transform = SqlTransform.query(configuration.query);
      if (configuration.dialect != null) {
        Class<? extends QueryPlanner> queryPlanner =
            DIALECTS.get(configuration.dialect.toLowerCase());
        if (queryPlanner == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Received unknown SQL Dialect '%s'. Known dialects: %s",
                  configuration.dialect, DIALECTS.keySet()));
        }
        transform = transform.withQueryPlannerClass(queryPlanner);
      }
      return transform;
    }
  }
}
