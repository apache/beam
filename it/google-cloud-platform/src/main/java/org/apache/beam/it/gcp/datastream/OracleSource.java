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
package org.apache.beam.it.gcp.datastream;

import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSchema;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.OracleTable;
import com.google.protobuf.MessageOrBuilder;
import java.util.List;
import java.util.Map;

/**
 * Client for Oracle resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class OracleSource extends JDBCSource {

  OracleSource(Builder builder) {
    super(builder);
  }

  public static Builder builder(
      String hostname,
      String username,
      String password,
      int port,
      Map<String, List<String>> allowedTables) {
    return new Builder(hostname, username, password, port, allowedTables);
  }

  @Override
  public SourceType type() {
    return SourceType.ORACLE;
  }

  @Override
  public MessageOrBuilder config() {
    OracleRdbms.Builder oracleRdmsBuilder = OracleRdbms.newBuilder();
    for (String schema : this.allowedTables().keySet()) {
      OracleSchema.Builder oracleSchemaBuilder = OracleSchema.newBuilder().setSchema(schema);
      for (String table : this.allowedTables().get(schema)) {
        oracleSchemaBuilder.addOracleTables(OracleTable.newBuilder().setTable(table));
      }
      oracleRdmsBuilder.addOracleSchemas(oracleSchemaBuilder);
    }
    return OracleSourceConfig.newBuilder().setIncludeObjects(oracleRdmsBuilder);
  }

  public static class Builder extends JDBCSource.Builder<OracleSource> {
    public Builder(
        String hostname,
        String username,
        String password,
        int port,
        Map<String, List<String>> allowedTables) {
      super(hostname, username, password, port, allowedTables);
    }

    @Override
    public OracleSource build() {
      return new OracleSource(this);
    }
  }
}
