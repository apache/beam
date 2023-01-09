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
package org.apache.beam.sdk.extensions.spd.profile.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(ProfileAdapter.class)
public class DefaultAdapter implements ProfileAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAdapter.class);

  @Override
  public String getName() {
    return "default";
  }

  @Override
  public Table getSourceTable(JsonNode profile, Table table) {
    LOG.info("({}) {}", table.getName(), profile.toPrettyString());
    JsonNode alter = profile.path("tables").path(table.getName());
    LOG.info("Profile table updates: {}", alter.toPrettyString());

    Table.Builder builder = table.toBuilder();

    JsonNode external = alter.path("external");
    if (external.has("location")) {
      builder.location(external.path("location").asText());
    }

    return builder.build();
  }

  @Override
  public Table getMaterializedTable(JsonNode profile, Table table) {
    return table;
  }
}
