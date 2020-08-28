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

import java.io.IOException;
import java.util.List;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Experimental(Kind.PORTABILITY)
public class WriteBuilder
    implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<byte[]>, PDone> {

  /** Parameters class to expose the transform to an external SDK. */
  public static class Configuration extends CrossLanguageConfiguration {
    private SnowflakeTableSchema tableSchema;
    private CreateDisposition createDisposition;
    private WriteDisposition writeDisposition;

    public void setTableSchema(String tableSchema) {
      ObjectMapper mapper = new ObjectMapper();

      try {
        this.tableSchema = mapper.readValue(tableSchema, SnowflakeTableSchema.class);
      } catch (IOException e) {
        throw new RuntimeException("Format of provided table schema is invalid");
      }
    }

    public void setCreateDisposition(String createDisposition) {
      this.createDisposition = CreateDisposition.valueOf(createDisposition);
    }

    public void setWriteDisposition(String writeDisposition) {
      this.writeDisposition = WriteDisposition.valueOf(writeDisposition);
    }

    public SnowflakeTableSchema getTableSchema() {
      return tableSchema;
    }

    public CreateDisposition getCreateDisposition() {
      return createDisposition;
    }

    public WriteDisposition getWriteDisposition() {
      return writeDisposition;
    }
  }

  @Override
  public PTransform<PCollection<byte[]>, PDone> buildExternal(Configuration c) {
    SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(c);

    SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(credentials)
            .withServerName(c.getServerName())
            .withDatabase(c.getDatabase())
            .withSchema(c.getSchema())
            .withRole(c.getRole())
            .withWarehouse(c.getWarehouse());

    return SnowflakeIO.<byte[]>write()
        .withDataSourceConfiguration(dataSourceConfiguration)
        .withStorageIntegrationName(c.getStorageIntegrationName())
        .withStagingBucketName(c.getStagingBucketName())
        .withTableSchema(c.getTableSchema())
        .withCreateDisposition(c.getCreateDisposition())
        .withWriteDisposition(c.getWriteDisposition())
        .withUserDataMapper(getStringCsvMapper())
        .withQueryTransformation(c.getQuery())
        .to(c.getTable());
  }

  private static SnowflakeIO.UserDataMapper<List<byte[]>> getStringCsvMapper() {
    return (SnowflakeIO.UserDataMapper<List<byte[]>>)
        recordLine -> recordLine.stream().map(String::new).toArray();
  }
}
