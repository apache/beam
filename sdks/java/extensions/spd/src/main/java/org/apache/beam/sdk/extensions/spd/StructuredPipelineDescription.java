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
package org.apache.beam.sdk.extensions.spd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hubspot.jinjava.Jinjava;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import org.apache.beam.sdk.extensions.spd.description.PackageImport;
import org.apache.beam.sdk.extensions.spd.description.Packages;
import org.apache.beam.sdk.extensions.spd.description.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredPipelineDescription {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineDescription.class);

  private ObjectMapper mapper;
  private Path path;
  private Jinjava jinjava;
  private HashMap<String, Object> bindings = new HashMap<String, Object>();

  @Nullable private Project project = null;

  public @Nullable String getName() {
    return project == null ? "" : project.name;
  }

  public @Nullable String getVersion() {
    return project == null ? "" : project.version;
  }

  public static ObjectMapper defaultObjectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  public StructuredPipelineDescription(ObjectMapper mapper, Jinjava jinjava, Path path) {
    this.mapper = mapper;
    this.jinjava = jinjava;
    this.path = path;
  }

  public StructuredPipelineDescription(Path path) {
    this(defaultObjectMapper(), new Jinjava(), path);
  }

  private @Nullable Path yamlPath(Path base, String yamlName) throws Exception {
    Path newPath = base.resolve(yamlName + File.separator + "yml");
    if (!Files.exists(newPath)) {
      newPath = base.resolve(yamlName + File.separator + "yaml");
    }
    return Files.exists(newPath) ? newPath : null;
  }

  private void initialize() throws Exception {
    bindings.clear();
    bindings.putIfAbsent(
        "tmpDir", Files.createTempDirectory("beamspd").toAbsolutePath().toString());
  }

  private void importPackages(Path path, Project project) throws Exception {
    Path packagePath = yamlPath(path, "packages");
    if (packagePath == null) return;

    Packages packages = mapper.readValue(Files.newBufferedReader(packagePath), Packages.class);
    for (PackageImport toImport : packages.packages) {
      if (toImport.local != null && !"".equals(toImport.local)) {
        Path localPath = Paths.get(jinjava.render(toImport.local, bindings));
        if (Files.exists(localPath)) loadPackage(localPath);
        else LOG.error("Package not found at '" + localPath.toAbsolutePath().toString() + "'");
      } else if (toImport.git != null && !"".equals(toImport.git)) {
        String tmpDir = jinjava.render(project.packagesInstallPath, bindings);
        LOG.warn(
            "Git imports currently unsupported. Skipping import of '"
                + toImport.git
                + "' to "
                + tmpDir);
      }
    }
  }

  private Project loadPackage(Path path) throws Exception {
    Path projectPath = yamlPath(path, "spd_project");
    if (projectPath == null)
      throw new Exception("spd_project file not found at path " + path.toString());
    Project project = mapper.readValue(Files.newBufferedReader(projectPath), Project.class);
    importPackages(path, project);
    return project;
  }

  public void load() throws Exception {
    initialize();
    project = loadPackage(path);
  }

  @Override
  public String toString() {
    return project == null ? "UNINITIALIZED_PROJECT" : "SPD:" + getName() + ":" + getVersion();
  }
}
