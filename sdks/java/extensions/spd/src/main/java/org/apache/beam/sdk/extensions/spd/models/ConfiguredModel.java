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
package org.apache.beam.sdk.extensions.spd.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.interpret.TemplateError;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.apache.beam.sdk.extensions.spd.macros.MacroContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfiguredModel {
  private static final Logger LOG = LoggerFactory.getLogger(ConfiguredModel.class);

  public static final String MATERIALIZED_CONFIG = "materialized";
  public static final String SQL_HEADER_CONFIG = "sql_header";
  public static final String ENABLED_CONFIG = "enabled";
  public static final String TAGS_CONFIG = "tags";
  public static final String PRE_HOOK_CONFIG = "pre-hook";
  public static final String POST_HOOK_CONFIG = "post-hook";
  public static final String DATABASE_CONFIG = "database";
  public static final String SCHEMA_CONFIG = "schema";
  public static final String ALIAS_CONFIG = "alias";
  public static final String PERSIST_DOCS_CONFIG = "persis_docs";
  public static final String FULL_REFRESH_CONFIG = "full_refresh";
  public static final String META_CONFIG = "meta";
  public static final String GRANTS_CONFIG = "grants";

  String name;
  Map<String, Object> config;

  public ConfiguredModel(String name, Map<String, Object> config) {
    this.name = name;
    this.config = config;
  }

  public ConfiguredModel(String name) {
    this(name, new HashMap<>());
  }

  public void mergeConfiguration(ObjectNode newConfig) {
    Iterator<String> fields = newConfig.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      // Configuration entries always start with a "+" sign
      if (field == null || !field.startsWith("+")) {
        continue;
      }
      // Chop off the first character
      String name = field.substring(1);
      LOG.info("Found config field " + name);
      if (MATERIALIZED_CONFIG.equals(name)) {
        config.put(MATERIALIZED_CONFIG, newConfig.get(field).asText());
      } else if (SQL_HEADER_CONFIG.equals(name)) {
        config.put(SQL_HEADER_CONFIG, newConfig.get(field).asText());
      } else if (ENABLED_CONFIG.equals(name)) {
        JsonNode node = newConfig.get(field);
        if (node.isBoolean()) {
          config.put(ENABLED_CONFIG, node.asBoolean());
        } else {
          config.put(ENABLED_CONFIG, node.asText());
        }
      } else if (TAGS_CONFIG.equals(name)) {
        LOG.warn("Setting " + TAGS_CONFIG + " not supported.");
      } else if (PRE_HOOK_CONFIG.equals(name)) {
        LOG.warn("Setting " + PRE_HOOK_CONFIG + " not supported.");
      } else if (POST_HOOK_CONFIG.equals(name)) {
        LOG.warn("Setting " + POST_HOOK_CONFIG + " not supported.");
      } else if (DATABASE_CONFIG.equals(name)) {
        config.put(DATABASE_CONFIG, newConfig.get(field).asText());
      } else if (SCHEMA_CONFIG.equals(name)) {
        config.put(SCHEMA_CONFIG, newConfig.get(field).asText());
      } else if (ALIAS_CONFIG.equals(name)) {
        config.put(ALIAS_CONFIG, newConfig.get(field).asText());
      } else if (PERSIST_DOCS_CONFIG.equals(name)) {
        LOG.warn("Setting " + PERSIST_DOCS_CONFIG + " not supported.");
      } else if (FULL_REFRESH_CONFIG.equals(name)) {
        config.put(FULL_REFRESH_CONFIG, newConfig.get(field).asBoolean());
      } else if (META_CONFIG.equals(name)) {
        LOG.warn("Setting " + META_CONFIG + " not supported.");
      } else if (GRANTS_CONFIG.equals(name)) {
        LOG.warn("Setting " + GRANTS_CONFIG + " not supported.");
      }
    }
  }

  public void mergeConfiguration(JsonNode newConfig) {
    if (newConfig != null && newConfig.isObject()) {
      mergeConfiguration((ObjectNode) newConfig);
    }
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean getEnabledConfig(MacroContext context, StructuredPipelineDescription spd)
      throws Exception {
    Object val = config.get(ENABLED_CONFIG);
    if (val == null) {
      return true;
    } else if (val instanceof Boolean) {
      return (Boolean) val;
    } else {
      RenderResult result = context.eval("" + val, spd);
      if (result.hasErrors()) {
        for (TemplateError error : result.getErrors()) {
          throw error.getException();
        }
      }
      // TODO: Implement macro support
      return true;
    }
  }

  public String getMaterializedConfig(MacroContext context, StructuredPipelineDescription spd)
      throws Exception {
    Object val = config.get(MATERIALIZED_CONFIG);
    if (val == null) {
      return "ephemeral";
    } else {
      RenderResult result = context.eval("" + val, spd);
      if (result.hasErrors()) {
        for (TemplateError error : result.getErrors()) {
          throw error.getException();
        }
      }
      return result.getOutput();
    }
  }
}
