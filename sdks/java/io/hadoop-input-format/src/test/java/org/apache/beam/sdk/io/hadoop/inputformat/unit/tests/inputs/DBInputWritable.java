package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.io.hadoop.inputformat.integration.tests.HIFIOWithPostgresIT;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * To access the data from RDBMS using {@link DBInputFormat} you have to create a class to define
 * the data which you are going to read. {@link DBInputWritable} is a class to read data from
 * Postgres DB in test {@link HIFIOWithPostgresIT}. DBInputWritable holds id and name of the
 * scientist.
 */
public class DBInputWritable implements Writable, DBWritable {
  public String id;
  public String name;

  /* 
   * Empty constuctor is required for encoding and decoding values in Beam.
   * Note: missing empty constuctor may result in RuntimeException  java.lang.NoSuchMethodException... <init>()
   */
  public DBInputWritable() {}

  public DBInputWritable(String id, String name) {
    this.id = id;
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  /*
   * Defines how the data had to be extracted from the DB. Both name and id field values are
   * extracted inthe form of string.
   */
  public void readFields(ResultSet rs) throws SQLException {
    name = rs.getString(1);
    id = rs.getString(2);
  }

  /*
   * Method is kept blank because the test does not aims to write back the data to Postgres.
   */
  public void write(PreparedStatement ps) {}

  /*
   * Deserialize the fields {id, name} of this object from in. Note: You must implement this method
   * for decoding in Beam. Leaving this method empty may result in incompatible value class in
   * Beam.
   */
  public void readFields(DataInput in) throws IOException {
    name = in.readUTF();
    id = in.readUTF();
  }

  /*
   * Serialize the fields {id, name} of this object to out. Note: You must implement this method
   * for encoding in Beam. Leaving this method empty may result in incompatible value class in
   * Beam.
   */
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
}
