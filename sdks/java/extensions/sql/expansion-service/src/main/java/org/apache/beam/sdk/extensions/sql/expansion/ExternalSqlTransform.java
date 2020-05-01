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
package org.apache.beam.sdk.extensions.sql;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;

@AutoService(ExternalTransformRegistrar.class)
public class ExternalSqlTransform implements ExternalTransformRegistrar {

  private static final String URN = "beam:external:java:sql:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap.of(
        URN, Builder.class);
  }

  public static class Configuration {
    String query;

    public void setQuery(String query) {
      this.query = query;
    }
  }

  private static class Builder
      implements ExternalTransformBuilder<Configuration, PInput, PCollection<Row>> {
    @Override
    public PTransform<PInput, PCollection<Row>> buildExternal(Configuration configuration) {
      Map<String, TableProvider> tableProviderMap =
          ImmutableMap.<String, TableProvider>builder()
              // TODO: can we get pipeline options passed through expansion request?
              .put(
                  "beam:tableprovider:datacatalog:v1",
                  DataCatalogTableProvider.create(
                      PipelineOptionsFactory.create().as(DataCatalogPipelineOptions.class)))
              .build();
      /*
      try {
        tableProviderMap = configuration.tableProviderMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                (Map.Entry<String, String> entry) -> Class.forName(entry.getValue())));
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("TableProvider class not found", e);
      }*/

      return SqlTransform.builder()
          .setQueryString(configuration.query)
          .setQueryParameters(QueryParameters.ofNone())
          .setUdafDefinitions(Collections.emptyList())
          .setUdfDefinitions(Collections.emptyList())
          .setTableProviderMap(tableProviderMap)
          .setDefaultTableProvider("beam:tableprovider:datacatalog:v1")
          .setAutoUdfUdafLoad(false)
          .build();
    }
  }
}
