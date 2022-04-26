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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.common.TestRow;

/**
 * Contains Test helper methods used by both Integration and Unit Tests in {@link
 * org.apache.beam.sdk.io.jdbc.JdbcIO}.
 */
class JdbcTestHelper {

  public static class TestDto extends JdbcWriteResult implements Serializable {

    public static final int EMPTY_RESULT = 0;

    private int simpleField;

    public TestDto(int simpleField) {
      this.simpleField = simpleField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestDto testDto = (TestDto) o;
      return simpleField == testDto.simpleField;
    }

    @Override
    public int hashCode() {
      return Objects.hash(simpleField);
    }
  }

  public static final Coder<TestDto> TEST_DTO_CODER =
      new CustomCoder<TestDto>() {
        @Override
        public void encode(TestDto value, OutputStream outStream)
            throws CoderException, IOException {
          BigEndianIntegerCoder.of().encode(value.simpleField, outStream);
        }

        @Override
        public TestDto decode(InputStream inStream) throws CoderException, IOException {
          int simpleField = BigEndianIntegerCoder.of().decode(inStream);
          return new TestDto(simpleField);
        }

        @Override
        public Object structuralValue(TestDto v) {
          return v;
        }
      };

  static class CreateTestRowOfNameAndId implements JdbcIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(
          Long.valueOf(resultSet.getLong("id")).intValue(), resultSet.getString("name"));
    }
  }

  static class PrepareStatementFromTestRow implements JdbcIO.PreparedStatementSetter<TestRow> {
    @Override
    public void setParameters(TestRow element, PreparedStatement statement) throws SQLException {
      statement.setLong(1, element.id());
      statement.setString(2, element.name());
    }
  }
}
