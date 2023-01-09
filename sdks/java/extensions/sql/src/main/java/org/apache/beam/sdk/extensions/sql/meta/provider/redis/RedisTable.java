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
package org.apache.beam.sdk.extensions.sql.meta.provider.redis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlOptions;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.*;

@SuppressWarnings({"nullness"})
public class RedisTable extends SchemaBaseBeamTable implements Serializable {

  // TODO: Add strict schema handling
  final List<String> idFields;
  final HashSet<String> ignored = new HashSet<>();
  final HashSet<String> included = new HashSet<>();

  final String tableName;
  final String authString;

  RedisTable(Table table) {
    super(table.getSchema());
    tableName = table.getName();
    authString = table.getLocation();

    // Scan the schema for primary key fields
    idFields = new ArrayList<>();
    for (Schema.Field f : table.getSchema().getFields()) {
      if (f.getOptions().hasOption(BeamSqlOptions.PrimaryKey)) {
        int pos = f.getOptions().getValueOrDefault(BeamSqlOptions.PrimaryKey, -1);
        if (pos >= 0) {
          idFields.set(pos, f.getName());
        }
      } else {
        // All other fields are included
        included.add(f.getName());
      }
    }

    // Ignore these fields for the purposes of writing
    for (String id : idFields) {
      if (id != null && !"".equals(id)) {
        ignored.add(id);
      }
    }
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    throw new UnsupportedOperationException(
        "Redis tables are currently only available for writing");
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(
                    (row) -> {
                      List<KV<String, String>> outputs = new ArrayList<>();

                      // Build the key name first
                      List<String> key = new ArrayList<>();
                      key.add(tableName);
                      for (String idField : idFields) {
                        if (idField != null && !"".equals(idField)) {
                          String fieldValue = row.getString(idField);
                          if (fieldValue == null) {
                            fieldValue = "";
                          }
                          key.add(fieldValue);
                        } else {
                          key.add("<undefined>");
                        }
                      }
                      String baseKey = String.join(":", key);

                      for (Schema.Field f : row.getSchema().getFields()) {
                        if (!ignored.contains(f.getName())) {
                          outputs.add(KV.of(baseKey + ":" + f.getName(), ""));
                        }
                      }

                      return outputs;
                    }))
        .apply(RedisIO.write().withMethod(RedisIO.Write.Method.SET).withAuth(authString));
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }
}
