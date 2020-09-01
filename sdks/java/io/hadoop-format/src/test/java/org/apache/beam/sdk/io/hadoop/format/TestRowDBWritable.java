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
package org.apache.beam.sdk.io.hadoop.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * A subclass of {@link TestRow} to be used with {@link
 * org.apache.hadoop.mapreduce.lib.db.DBInputFormat}.
 */
@DefaultCoder(AvroCoder.class)
class TestRowDBWritable extends TestRow implements DBWritable, Writable {

  private Integer id;
  private String name;

  public TestRowDBWritable() {}

  public TestRowDBWritable(Integer id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public Integer id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void write(PreparedStatement statement) throws SQLException {
    statement.setInt(1, id);
    statement.setString(2, name);
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    id = resultSet.getInt(1);
    name = resultSet.getString(2);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeChars(name);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    name = in.readUTF();
  }

  private static class PrepareStatementFromTestRow
      implements JdbcIO.PreparedStatementSetter<TestRow> {
    @Override
    public void setParameters(TestRow element, PreparedStatement statement) throws SQLException {
      statement.setLong(1, element.id());
      statement.setString(2, element.name());
    }
  }
}
