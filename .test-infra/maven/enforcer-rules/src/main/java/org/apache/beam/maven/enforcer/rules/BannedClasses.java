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

package org.apache.beam.maven.enforcer.rules;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.enforcer.rule.api.EnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.enforcer.rule.api.EnforcerRuleHelper;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.shade.filter.SimpleFilter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.component.configurator.expression.ExpressionEvaluationException;

/**
 * A custom {@link EnforcerRule} that looks at the artifact jar to ensure it does not contain any
 * of the banned classes provided as input to this rule.
 *
 * <p>The banned class is specified as a relative path in the artifact jar. A fully qualified class
 * file path or a wildcard path (ending with / or **) is allowed. For example:
 * <ul>
 *   <li>a/b/c/d.class</li>
 *   <li>m/n/**</li>
 *   <li>x/y/z/</li>
 * </ul>
 *
 * <p>Note: This rule is specifically useful for uber jars.</p>
 */
public class BannedClasses implements EnforcerRule {

  private String[] excludes;

  @Override
  public void execute(EnforcerRuleHelper helper) throws EnforcerRuleException {
    Log log = helper.getLog();
    log.info("Executing BannedClasses enforcer rule.");
    try {
      MavenProject project = (MavenProject) helper.evaluate("${project}");

      // Find banned classes from the current artifact.
      if (excludes != null) {
        Set<String> bannedClassesInArtifact = findBannedClasses(project.getArtifact(), excludes);
        if (!bannedClassesInArtifact.isEmpty()) {
          throw new EnforcerRuleException(
              String.format("Found following banned classes in artifact %s: \n%s",
                  project.getArtifact(), bannedClassesInArtifact));
        }
      }
    } catch (ExpressionEvaluationException e) {
      throw new EnforcerRuleException(
          "Unable to lookup an expression " + e.getLocalizedMessage(), e);
    }
  }

  private static Set<String> findBannedClasses(Artifact artifact, String[] bannedClasses)
      throws EnforcerRuleException {
    Set<String> found = new HashSet<>();
    if (artifact.getFile() != null
        && artifact.getFile().isFile()
        && "jar".equals(artifact.getType())) {
      try (JarFile jarFile = new JarFile(artifact.getFile())) {
        // A filter that excludes the specified banned classes. It matches the full path or a
        // wildcard path that ends with / or **.
        SimpleFilter filter = new SimpleFilter(null, null,
            bannedClasses == null ? null : new HashSet(Arrays.asList(bannedClasses)));

        for (JarEntry entry: Collections.list(jarFile.entries())) {
          if (!entry.isDirectory() && filter.isFiltered(entry.getName())) {
            found.add(entry.getName());
          }
        }
      } catch (IOException e) {
        throw new EnforcerRuleException("Cannot find artifact jar " + e.getLocalizedMessage(), e);
      }
    } else {
      // If artifact file is empty when executed for an empty parent pom, we just ignore them.
      if (artifact.getFile() != null) {
        String message = String.format("The artifact type should be a jar, but found: '%s' "
                + "of type: %s. Make sure that this rule is bound to a phase that includes "
                + "'package' phase",
            artifact.getFile(), artifact.getType());
        throw new EnforcerRuleException(message);
      }
    }
    return found;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  @Override
  public boolean isResultValid(EnforcerRule cachedRule) {
    return false;
  }

  @Override
  public String getCacheId() {
    return null;
  }
}
