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
package org.apache.beam.sdk.io.jdbc;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link JdbcIO.ReadRows} as an external transform for cross-language usage. */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class JdbcReadRowsRegistrar implements ExternalTransformRegistrar {

  public static final String URN = "beam:external:java:jdbc:read_rows:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, JdbcReadRowsRegistrar.Builder.class);
  }

  /** Parameters class to expose the Read transform to an external SDK. */
  public static class ReadConfiguration extends CrossLanguageConfiguration {
    private String query;
    private Integer fetchSize;
    private Boolean outputParallelization;

    public void setOutputParallelization(Boolean outputParallelization) {
      this.outputParallelization = outputParallelization;
    }

    public void setFetchSize(Integer fetchSize) {
      this.fetchSize = fetchSize;
    }

    public void setQuery(String query) {
      this.query = query;
    }
  }

  public static class Builder
      implements ExternalTransformBuilder<ReadConfiguration, PBegin, PCollection<Row>> {
    @Override
    public PTransform<PBegin, PCollection<Row>> buildExternal(ReadConfiguration configuration) {
      DataSourceConfiguration dataSourceConfiguration = configuration.getDataSourceConfiguration();

      JdbcIO.ReadRows readRows =
          JdbcIO.readRows()
              .withDataSourceConfiguration(dataSourceConfiguration)
              .withQuery(configuration.query);

      if (configuration.fetchSize != null) {
        readRows = readRows.withFetchSize(configuration.fetchSize);
      }
      if (configuration.outputParallelization != null) {
        readRows = readRows.withOutputParallelization(configuration.outputParallelization);
      }
      return readRows;
    }
  }
}
