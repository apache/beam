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
package org.apache.beam.sdk.io.cdap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.plugin.PluginConfig;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for building {@link PluginConfig} object of the specific class {@param <T>}. */
public class ConfigWrapper<T extends PluginConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigWrapper.class);

  @Nullable private Map<String, Object> paramsMap = null;
  private final Class<T> configClass;

  public ConfigWrapper(Class<T> configClass) {
    this.configClass = configClass;
  }

  /** Gets {@link ConfigWrapper} by JSON string. */
  public ConfigWrapper<T> fromJsonString(String jsonString) throws IOException {
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    try {
      paramsMap = new ObjectMapper().readValue(jsonString, typeRef);
    } catch (IOException e) {
      LOG.error("Can not read json string to params map", e);
      throw e;
    }
    return this;
  }

  /** Gets {@link ConfigWrapper} by JSON file. */
  public ConfigWrapper<T> fromJsonFile(File jsonFile) throws IOException {
    TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};
    try {
      paramsMap = new ObjectMapper().readValue(jsonFile, typeRef);
    } catch (IOException e) {
      LOG.error("Can not read json file to params map", e);
      throw e;
    }
    return this;
  }

  /** Sets a {@link Plugin} parameters {@link Map}. */
  public ConfigWrapper<T> withParams(Map<String, Object> paramsMap) {
    this.paramsMap = new HashMap<>(paramsMap);
    return this;
  }

  /** Sets a {@link Plugin} single parameter. */
  public ConfigWrapper<T> setParam(String paramName, Object param) {
    getParamsMap().put(paramName, param);
    return this;
  }

  public @Nullable T build() {
    return PluginConfigInstantiationUtils.getPluginConfig(getParamsMap(), configClass);
  }

  private @Nonnull Map<String, Object> getParamsMap() {
    if (paramsMap == null) {
      paramsMap = new HashMap<>();
    }
    return paramsMap;
  }
}
