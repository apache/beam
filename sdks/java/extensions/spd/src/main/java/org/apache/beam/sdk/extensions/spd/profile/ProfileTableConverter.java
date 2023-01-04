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
package org.apache.beam.sdk.extensions.spd.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.extensions.spd.description.Source;
import org.apache.beam.sdk.extensions.spd.description.TableDesc;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

public abstract class ProfileTableConverter {
  private static Set<String> RESERVED_FIELDS = Sets.newHashSet("tables", "type");
  JsonNode profile;
  ObjectMapper mapper;

  public ProfileTableConverter(JsonNode profile) {
    this.profile = profile;
    this.mapper = new ObjectMapper();
  }

  protected ObjectNode profileForTable(String table) {
    ObjectNode config = mapper.createObjectNode();
    // Merge in the table first because we use putIfAbsent
    JsonNode tableNode = profile.path("tables").path(table);
    for (Iterator<String> fields = tableNode.fieldNames(); fields.hasNext(); ) {
      String field = fields.next();
      config.putIfAbsent(field, tableNode.path(field));
    }
    // Now add in the top-level profile settings
    for (Iterator<String> fields = profile.fieldNames(); fields.hasNext(); ) {
      String field = fields.next();
      if (!RESERVED_FIELDS.contains(field)) {
        config.putIfAbsent(field, profile.path(field));
      }
    }
    return config;
  }

  public abstract String convertsType();

  public abstract Table getSourceTable(String name, Source source, TableDesc desc);

  public abstract Table getMaterializedTable(String name);
}
