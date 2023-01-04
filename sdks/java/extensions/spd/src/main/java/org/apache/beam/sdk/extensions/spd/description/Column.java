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
package org.apache.beam.sdk.extensions.spd.description;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;

@SuppressFBWarnings
public class Column {

  @Nullable
  @JsonSetter(nulls = Nulls.FAIL)
  public String name;

  public String getName() {
    return name == null ? "" : name;
  }

  @Nullable public String description;

  public String getDescription() {
    return description == null ? "" : description;
  }

  @Nullable
  @JsonProperty("data_type")
  public String type;

  @Nullable
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  public List<String> tests = Arrays.asList();

  public List<String> getTests() {
    return tests == null ? Arrays.asList() : tests;
  }

  public static Schema asSchema(Iterable<Column> columns) {
    Schema.Builder beamSchema = Schema.builder();
    for (Column c : columns) {
      HashSet<String> tests = new HashSet<>();
      tests.addAll(c.getTests());
      if (c.type != null && c.name != null) {
        Schema.FieldType type = Schema.FieldType.of(Schema.TypeName.valueOf(c.type));
        if (tests.contains("not_null")) {
          beamSchema.addField(c.getName(), type);
        } else {
          beamSchema.addNullableField(c.getName(), type);
        }
      }
    }
    return beamSchema.build();
  }
}
