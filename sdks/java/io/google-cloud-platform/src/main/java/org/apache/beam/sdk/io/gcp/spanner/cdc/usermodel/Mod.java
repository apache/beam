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
package org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Mod implements Serializable {

  private static final long serialVersionUID = 7362322548913179939L;

  private Map<String, String> oldValues;
  private Map<String, String> newValues;

  public static OldAndNewBuilder newOldAndNewBuilder() {
    return new OldAndNewBuilder();
  }

  public Mod() {}

  public Mod(Map<String, String> oldValues, Map<String, String> newValues) {
    this.oldValues = oldValues;
    this.newValues = newValues;
  }

  public Map<String, String> getOldValues() {
    return oldValues;
  }

  public void setOldValues(Map<String, String> oldValues) {
    this.oldValues = oldValues;
  }

  public Map<String, String> getNewValues() {
    return newValues;
  }

  public void setNewValues(Map<String, String> newValues) {
    this.newValues = newValues;
  }

  public static class OldAndNewBuilder {
    private final Map<String, String> oldValues;
    private final Map<String, String> newValues;

    public OldAndNewBuilder() {
      this.oldValues = new HashMap<>();
      this.newValues = new HashMap<>();
    }

    public OldAndNewBuilder addValue(String name, String oldValue, String newValue) {
      oldValues.put(name, oldValue);
      newValues.put(name, newValue);
      return this;
    }

    public Mod build() {
      return new Mod(oldValues, newValues);
    }
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
    return Objects.equals(newValues, mod.newValues) && Objects.equals(oldValues, mod.oldValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newValues, oldValues);
  }
}
