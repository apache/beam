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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import java.io.Serializable;
import java.util.Map;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;

@DefaultCoder(AvroCoder.class)
public class Mod implements Serializable {

  private static final long serialVersionUID = 7362322548913179939L;
  private static final String NULLS_ALLOWED_SCHEMA =
      "[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"string\"]}]";

  private Map<String, String> keys;

  @AvroSchema(NULLS_ALLOWED_SCHEMA)
  private Map<String, String> oldValues;

  @AvroSchema(NULLS_ALLOWED_SCHEMA)
  private Map<String, String> newValues;

  /** Default constructor for serialization only. */
  private Mod() {}

  public Mod(
      Map<String, String> keys, Map<String, String> oldValues, Map<String, String> newValues) {
    this.keys = keys;
    this.oldValues = oldValues;
    this.newValues = newValues;
  }

  public Map<String, String> getOldValues() {
    return oldValues;
  }

  public Map<String, String> getNewValues() {
    return newValues;
  }

  public Map<String, String> getKeys() {
    return keys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Mod mod = (Mod) o;
    return Objects.equal(getKeys(), mod.getKeys())
        && Objects.equal(getOldValues(), mod.getOldValues())
        && Objects.equal(getNewValues(), mod.getNewValues());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getKeys(), getOldValues(), getNewValues());
  }

  @Override
  public String toString() {
    return "Mod{" + "keys=" + keys + ", oldValues=" + oldValues + ", newValues=" + newValues + '}';
  }
}
