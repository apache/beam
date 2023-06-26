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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("nullness")
@AutoValue
public abstract class ExpansionServiceConfig {
  public abstract List<String> getAllowlist();

  public abstract Map<String, List<Dependency>> getDependencies();

  public static ExpansionServiceConfig empty() {
    return create(new ArrayList<>(), new HashMap<>());
  }

  @JsonCreator
  static ExpansionServiceConfig create(
      @JsonProperty("allowlist") List<String> allowlist,
      @JsonProperty("dependencies") Map<String, List<Dependency>> dependencies) {
    if (allowlist == null) {
      allowlist = new ArrayList<>();
    }
    System.out.println("Dependencies list: " + dependencies);
    return new AutoValue_ExpansionServiceConfig(allowlist, dependencies);
  }
}
