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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * Represents a modification in a table emitted within a {@link DataChangeRecord}. Each mod contains
 * keys, new values and old values returned as JSON strings.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class Mod implements Serializable {

  private static final long serialVersionUID = 7362322548913179939L;

  private String keysJson;

  @Nullable @org.apache.avro.reflect.Nullable private String oldValuesJson;

  @Nullable @org.apache.avro.reflect.Nullable private String newValuesJson;

  /** Default constructor for serialization only. */
  private Mod() {}

  /**
   * Constructs a mod from the primary key values, the old state of the row and the new state of the
   * row.
   *
   * @param keysJson JSON object as String, where the keys are the primary key column names and the
   *     values are the primary key column values
   * @param oldValuesJson JSON object as String, displaying the old state of the columns modified.
   *     This JSON object can be null in the case of an INSERT
   * @param newValuesJson JSON object as String, displaying the new state of the columns modified.
   *     This JSON object can be null in the case of a DELETE
   */
  public Mod(String keysJson, @Nullable String oldValuesJson, @Nullable String newValuesJson) {
    this.keysJson = keysJson;
    this.oldValuesJson = oldValuesJson;
    this.newValuesJson = newValuesJson;
  }

  /**
   * The old column values before the modification was applied. This can be null when the
   * modification was emitted for an INSERT operation. The values are returned as a JSON object
   * (stringified), where the keys are the column names and the values are the column values.
   *
   * @return JSON object as String representing the old column values before the row was modified
   */
  public @Nullable String getOldValuesJson() {
    return oldValuesJson;
  }

  /**
   * The new column values after the modification was applied. This can be null when the
   * modification was emitted for a DELETE operation. The values are returned as a JSON object
   * (stringified), where the keys are the column names and the values are the column values.
   *
   * @return JSON object as String representing the new column values after the row was modified
   */
  public @Nullable String getNewValuesJson() {
    return newValuesJson;
  }

  /**
   * The primary keys of this specific modification. This is always present and can not be null. The
   * keys are returned as a JSON object (stringified), where the keys are the column names and the
   * values are the column values.
   *
   * @return JSON object as String representing the primary key state for the row modified
   */
  public String getKeysJson() {
    return keysJson;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mod)) {
      return false;
    }
    Mod mod = (Mod) o;
    return Objects.equals(keysJson, mod.keysJson)
        && Objects.equals(oldValuesJson, mod.oldValuesJson)
        && Objects.equals(newValuesJson, mod.newValuesJson);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keysJson, oldValuesJson, newValuesJson);
  }

  @Override
  public String toString() {
    return "Mod{"
        + "keysJson="
        + keysJson
        + ", oldValuesJson='"
        + oldValuesJson
        + '\''
        + ", newValuesJson='"
        + newValuesJson
        + '\''
        + '}';
  }
}
