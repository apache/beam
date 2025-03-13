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

import com.google.cloud.datastream.v1.MysqlDatabase;
import com.google.cloud.datastream.v1.MysqlRdbms;
import com.google.cloud.datastream.v1.MysqlSourceConfig;
import com.google.cloud.datastream.v1.MysqlTable;
import java.util.List;
import java.util.Map;

/**
 * Client for MySQL resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class MySQLSource extends JDBCSource {

  MySQLSource(Builder builder) {
    super(builder);
  }

  @Override
  public SourceType type() {
    return SourceType.MYSQL;
  }

  @Override
  public MysqlSourceConfig config() {
    MysqlRdbms.Builder mysqlRdmsBuilder = MysqlRdbms.newBuilder();
    for (String db : this.allowedTables().keySet()) {
      MysqlDatabase.Builder mysqlDbBuilder = MysqlDatabase.newBuilder().setDatabase(db);
      for (String table : this.allowedTables().get(db)) {
        mysqlDbBuilder.addMysqlTables(MysqlTable.newBuilder().setTable(table));
      }
      mysqlRdmsBuilder.addMysqlDatabases(mysqlDbBuilder);
    }
    return MysqlSourceConfig.newBuilder().setIncludeObjects(mysqlRdmsBuilder).build();
  }

  public static Builder builder(
      String hostname,
      String username,
      String password,
      int port,
      Map<String, List<String>> allowedTables) {
    return new Builder(hostname, username, password, port, allowedTables);
  }

  public static class Builder extends JDBCSource.Builder<MySQLSource> {
    public Builder(
        String hostname,
        String username,
        String password,
        int port,
        Map<String, List<String>> allowedTables) {
      super(hostname, username, password, port, allowedTables);
    }

    @Override
    public MySQLSource build() {
      return new MySQLSource(this);
    }
  }
}
