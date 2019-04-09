/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import org.gradle.api.Project
import org.gradle.util.Configurable
import org.gradle.util.ConfigureUtil


/**
 * Container for maintaining beam projects dependency versions.
 * <p>
 * A configured version might be overridden by a project property
 * <pre>
 *   ./gradlew test -Psome_lib.version=1.1.0
 * </pre>
 */
class Versions implements Configurable<Versions> {

  Project project
  def versionMap = [:]

  Versions(Project project) {
    this.project = project
  }

  def propertyMissing(String name, String value) {
    versionMap[name] = propertyOrValue(name, value)
  }

  def propertyMissing(String name) {
    if(versionMap.containsKey(name)) {
      return versionMap[name]
    }
    throw new MissingPropertyException(name, Versions)
  }

  def methodMissing(String name, def args) {
    if((!args || args.length == 0) && versionMap.containsKey(name)) {
      return versionMap[name]
    }

    if(args && args.length == 1) {
      versionMap[name] = propertyOrValue(name, args[0] as String)
      return versionMap[name]
    }
    throw new MissingMethodException(name, Versions, (Object[])args)
  }

  String toString() {
    return versionMap as String
  }

  private propertyOrValue(String name, String value) {
    project.findProperty("${name}.version") ?: value
  }

  Versions configure(Closure closure)  {
    ConfigureUtil.configureSelf(closure, this)
  }
}
