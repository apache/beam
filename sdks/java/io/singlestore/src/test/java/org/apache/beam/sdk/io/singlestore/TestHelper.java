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
package org.apache.beam.sdk.io.singlestore;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.common.TestRow;

public class TestHelper {
  public static class TestRowMapper implements SingleStoreIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(resultSet.getInt(1), resultSet.getString(2));
    }
  }

  public static class TestUserDataMapper implements SingleStoreIO.UserDataMapper<TestRow> {
    @Override
    public List<String> mapRow(TestRow element) {
      List<String> res = new ArrayList<>();
      res.add(element.id().toString());
      res.add(element.name());
      return res;
    }
  }

  public abstract static class MockDataSourceConfiguration
      extends SingleStoreIO.DataSourceConfiguration {
    @Override
    String getEndpoint() {
      return "localhost";
    }

    @Override
    String getDatabase() {
      return "db";
    }

    @Override
    String getConnectionProperties() {
      return "";
    }

    @Override
    String getUsername() {
      return "admin";
    }

    @Override
    String getPassword() {
      return "secretPass";
    }
  }
}
