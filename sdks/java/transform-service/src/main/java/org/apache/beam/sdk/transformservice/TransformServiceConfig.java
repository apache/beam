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
package org.apache.beam.sdk.transformservice;

import com.google.auto.value.AutoValue;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

@AutoValue
public abstract class TransformServiceConfig {
  public abstract List<String> getExpansionservices();

  public static TransformServiceConfig empty() {
    return create(new ArrayList<>());
  }

  static TransformServiceConfig create(List<String> expansionservices) {
    if (expansionservices == null) {
      expansionservices = new ArrayList<>();
    }

    return new AutoValue_TransformServiceConfig(expansionservices);
  }

  static TransformServiceConfig parseFromYamlStream(InputStream inputStream) {
    Yaml yaml = new Yaml();
    Map<Object, Object> config = yaml.load(inputStream);
    if (config == null) {
      throw new IllegalArgumentException(
          "Could not parse the provided YAML stream into a non-trivial TransformServiceConfig");
    }

    List<String> expansionservices = null;
    if (config.get("expansionservices") != null) {
      expansionservices = (List<String>) config.get("expansionservices");
    }

    if (expansionservices == null) {
      throw new IllegalArgumentException(
          "Expected the Transform Service config to contain at least one Expansion Service.");
    }

    return TransformServiceConfig.create(expansionservices);
  }
}
