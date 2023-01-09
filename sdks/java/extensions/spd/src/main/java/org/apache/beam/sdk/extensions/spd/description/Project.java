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
import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;
import java.util.List;

public class Project {

  @JsonSetter(nulls = Nulls.FAIL)
  @Nullable
  public String name;

  @JsonSetter(nulls = Nulls.FAIL)
  @Nullable
  public String version;

  @JsonSetter(nulls = Nulls.FAIL)
  @Nullable
  public String profile;

  @JsonProperty("model-paths")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> modelPaths = Arrays.asList("models");

  @JsonProperty("seed-paths")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> seedPaths = Arrays.asList("seeds");

  @JsonProperty("test-paths")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> testPaths = Arrays.asList("tests");

  @JsonProperty("macro-paths")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> macrosPaths = Arrays.asList("macros");

  @JsonProperty("docs-paths")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> docsPaths = Arrays.asList("docs");

  @JsonProperty("reports-path")
  @JsonSetter(nulls = Nulls.SKIP)
  public List<String> reportsPaths = Arrays.asList("reports");

  @JsonProperty("target-path")
  @JsonSetter(nulls = Nulls.SKIP)
  public String targetPath = "target";

  @JsonProperty("packages-install-path")
  @JsonSetter(nulls = Nulls.SKIP)
  public String packagesInstallPath = "{{tmpDir}}";

  // Configuration objects

  @Nullable
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  public JsonNode vars;

  @Nullable public JsonNode models;

  @Nullable
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  public JsonNode sources;

  @Nullable
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  public JsonNode seeds;

  @Nullable
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  public JsonNode tests;
}
