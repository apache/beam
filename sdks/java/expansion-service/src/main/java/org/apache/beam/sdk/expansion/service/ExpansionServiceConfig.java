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
package org.apache.beam.sdk.expansion.service;

import com.google.auto.value.AutoValue;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.yaml.snakeyaml.Yaml;

@SuppressWarnings("nullness")
@AutoValue
public abstract class ExpansionServiceConfig {

  public abstract List<String> getAllowlist();

  public abstract Map<String, List<Dependency>> getDependencies();

  public static ExpansionServiceConfig empty() {
    return create(new ArrayList<>(), new HashMap<>());
  }

  public static ExpansionServiceConfig create(
      List<String> allowlist, Map<String, List<Dependency>> dependencies) {
    if (allowlist == null) {
      allowlist = new ArrayList<>();
    }
    return new AutoValue_ExpansionServiceConfig(allowlist, dependencies);
  }

  static ExpansionServiceConfig parseFromYamlStream(InputStream inputStream) {
    Yaml yaml = new Yaml();
    Map<Object, Object> config = yaml.load(inputStream);

    if (config == null) {
      throw new IllegalArgumentException(
          "Could not parse the provided YAML stream into a non-trivial ExpansionServiceConfig");
    }

    List<String> allowList = new ArrayList<>();
    Map<String, List<Dependency>> dependencies = new HashMap<>();
    if (config.get("allowlist") != null) {
      allowList = (List<String>) config.get("allowlist");
    }

    if (config.get("dependencies") != null) {
      Map<String, List<Object>> dependenciesFromConfig =
          (Map<String, List<Object>>) config.get("dependencies");
      dependenciesFromConfig.forEach(
          (k, v) -> {
            if (v != null) {
              List<Dependency> dependenciesForTransform =
                  v.stream()
                      .map(
                          val -> {
                            Map<String, Object> depProperties = (Map<String, Object>) val;
                            String path = (String) depProperties.get("path");
                            if (path == null) {
                              throw new IllegalArgumentException(
                                  "Expected the path to be not null");
                            }
                            return Dependency.create(path);
                          })
                      .collect(Collectors.toList());
              dependencies.put(k, dependenciesForTransform);
            }
          });
    }
    return ExpansionServiceConfig.create(allowList, dependencies);
  }
}
