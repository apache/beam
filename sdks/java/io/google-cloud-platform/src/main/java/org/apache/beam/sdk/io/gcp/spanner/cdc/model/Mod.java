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
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Mod implements Serializable {

  private static final long serialVersionUID = 7362322548913179939L;

  private String keysJson;

  @Nullable private String oldValuesJson;

  @Nullable private String newValuesJson;

  /** Default constructor for serialization only. */
  private Mod() {}

  public Mod(String keysJson, String oldValuesJson, String newValuesJson) {
    this.keysJson = keysJson;
    this.oldValuesJson = oldValuesJson;
    this.newValuesJson = newValuesJson;
  }

  public String getOldValuesJson() {
    return oldValuesJson;
  }

  public String getNewValuesJson() {
    return newValuesJson;
  }

  public String getKeysJson() {
    return keysJson;
  }

  @Override
  public boolean equals(Object o) {
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
