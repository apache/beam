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
package org.apache.beam.sdk.io.snowflake.crosslanguage;

import java.io.Serializable;
import java.nio.charset.Charset;
import javax.sql.DataSource;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

@Experimental(Kind.PORTABILITY)
public class ReadBuilder
    implements ExternalTransformBuilder<ReadBuilder.Configuration, PBegin, PCollection<byte[]>> {

  /** Parameters class to expose the transform to an external SDK. */
  public static class Configuration extends CrossLanguageConfiguration {}

  @Override
  public PTransform<PBegin, PCollection<byte[]>> buildExternal(Configuration c) {
    SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(c);

    SerializableFunction<Void, DataSource> dataSourceSerializableFunction =
        SnowflakeIO.DataSourceProviderFromDataSourceConfiguration.of(
            SnowflakeIO.DataSourceConfiguration.create(credentials)
                .withServerName(c.getServerName())
                .withDatabase(c.getDatabase())
                .withSchema(c.getSchema())
                .withRole(c.getRole())
                .withWarehouse(c.getWarehouse()));

    return SnowflakeIO.<byte[]>read()
        .withStorageIntegrationName(c.getStorageIntegrationName())
        .withStagingBucketName(c.getStagingBucketName())
        .withDataSourceProviderFn(dataSourceSerializableFunction)
        .withCsvMapper(CsvMapper.getCsvMapper())
        .withCoder(ByteArrayCoder.of())
        .fromTable(c.getTable())
        .fromQuery(c.getQuery());
  }

  private static class CsvMapper implements Serializable {

    public static SnowflakeIO.CsvMapper getCsvMapper() {
      return (SnowflakeIO.CsvMapper<byte[]>)
          parts -> {
            String partsCSV = String.join(",", parts);

            return partsCSV.getBytes(Charset.defaultCharset());
          };
    }
  }
}
