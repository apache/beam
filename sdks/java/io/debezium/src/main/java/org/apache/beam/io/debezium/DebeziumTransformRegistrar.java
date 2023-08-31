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
package org.apache.beam.io.debezium;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Exposes {@link DebeziumIO.Read} as an external transform for cross-language usage. */
@AutoService(ExternalTransformRegistrar.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DebeziumTransformRegistrar implements ExternalTransformRegistrar {
  public static final String READ_JSON_URN = "beam:transform:org.apache.beam:debezium_read:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(
        READ_JSON_URN,
        (Class<? extends ExternalTransformBuilder<?, ?, ?>>) (Class<?>) ReadBuilder.class);
  }

  private abstract static class CrossLanguageConfiguration {
    String username;
    String password;
    String host;
    String port;
    Connectors connectorClass;

    public void setUsername(String username) {
      this.username = username;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setPort(String port) {
      this.port = port;
    }

    public void setConnectorClass(String connectorClass) {
      this.connectorClass = Connectors.fromName(connectorClass);
    }
  }

  public static class ReadBuilder
      implements ExternalTransformBuilder<ReadBuilder.Configuration, PBegin, PCollection<String>> {

    public static class Configuration extends CrossLanguageConfiguration {
      private @Nullable List<String> connectionProperties;
      private @Nullable Long maxNumberOfRecords;

      public void setConnectionProperties(@Nullable List<String> connectionProperties) {
        this.connectionProperties = connectionProperties;
      }

      public void setMaxNumberOfRecords(@Nullable Long maxNumberOfRecords) {
        this.maxNumberOfRecords = maxNumberOfRecords;
      }
    }

    @Override
    public PTransform<PBegin, PCollection<String>> buildExternal(Configuration configuration) {
      DebeziumIO.ConnectorConfiguration connectorConfiguration =
          DebeziumIO.ConnectorConfiguration.create()
              .withUsername(configuration.username)
              .withPassword(configuration.password)
              .withHostName(configuration.host)
              .withPort(configuration.port)
              .withConnectorClass(configuration.connectorClass.getConnector());

      if (configuration.connectionProperties != null) {
        for (String connectionProperty : configuration.connectionProperties) {
          String[] parts = connectionProperty.split("=", -1);
          String key = parts[0];
          String value = parts[1];
          connectorConfiguration.withConnectionProperty(key, value);
        }
      }

      DebeziumIO.Read<String> readTransform =
          DebeziumIO.readAsJson().withConnectorConfiguration(connectorConfiguration);

      if (configuration.maxNumberOfRecords != null) {
        readTransform =
            readTransform.withMaxNumberOfRecords(configuration.maxNumberOfRecords.intValue());
      }

      return readTransform;
    }
  }
}
