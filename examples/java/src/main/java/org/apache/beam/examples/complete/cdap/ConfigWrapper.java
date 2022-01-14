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
package org.apache.beam.examples.complete.cdap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.plugin.PluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for building {@link PluginConfig} object of the specific class {@param <T>}.
 */
public class ConfigWrapper<T extends PluginConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigWrapper.class);

    private Map<String, Object> paramsMap;
    private final Class<T> configClass;

    public ConfigWrapper(Class<T> configClass) {
        this.configClass = configClass;
    }

    public ConfigWrapper<T> fromJsonFile(File jsonFile) {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            paramsMap = new ObjectMapper()
                    .readValue(jsonFile, typeRef);
        } catch (IOException e) {
            LOG.error("Can not read json file to params map", e);
        }
        return this;
    }

    public ConfigWrapper<T> fromJsonString(String jsonString) {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            paramsMap = new ObjectMapper()
                    .readValue(jsonString, typeRef);
        } catch (IOException e) {
            LOG.error("Can not read json string to params map", e);
        }
        return this;
    }

    public ConfigWrapper<T> withParams(Map<String, Object> paramsMap) {
        this.paramsMap = new HashMap<>(paramsMap);
        return this;
    }

    public ConfigWrapper<T> setParam(String paramName, Object param) {
        getParamsMap().put(paramName, param);
        return this;
    }

    public T build() {
        return PluginConfigInstantiationUtils.getPluginConfig(getParamsMap(), configClass);
    }

    private Map<String, Object> getParamsMap() {
        if (paramsMap == null) {
            paramsMap = new HashMap<>();
        }
        return paramsMap;
    }
}