/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable {
  public String id;
  public String name;

  public DBInputWritable() {}

  public DBInputWritable(String id, String name) {
    this.id = id;
    this.name = name;
  }

  public void readFields(DataInput in) throws IOException {
    name = in.readUTF();
    id = in.readUTF();
  }

  public void readFields(ResultSet rs) throws SQLException
  // Resultset object represents the data returned from a SQL statement
  {
    name = rs.getString(1);
    id = rs.getString(2);
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeUTF(id);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DBInputWritable other = (DBInputWritable) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  public void write(PreparedStatement ps) throws SQLException {
    ps.setString(1, name);
    ps.setString(2, id);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
