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
package org.apache.beam.gradle.rust

import groovy.transform.CompileStatic
import org.apache.beam.gradle.BeamModulePlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Exec

/**
 * The build of the Rust SDK ({@code :sdks:rust}).
 *
 * <p>Cargo does the work. The tasks give maintainers the same commands as the other SDKs:
 * {@code build}, {@code check}, {@code test}, {@code fmt} and {@code clean}.
 */
@CompileStatic
class BeamRustPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    // BeamModulePlugin sets the project version and adds the lifecycle tasks.
    project.pluginManager.apply(BeamModulePlugin)

    RustSdkExtension extension = project.extensions.create('rustSdk', RustSdkExtension)
    extension.benchmarkPackages.convention(new ArrayList<String>())
    extension.benchmarkPackages.finalizeValueOnRead()

    RustSdkBuild build = new RustSdkBuild(project, extension)
    Map<String, String> cargoEnv = build.versions.cargoEnv()
    project.tasks.withType(Exec).configureEach { Exec task -> task.environment(cargoEnv) }

    RustLintTasks.register(build)
    RustTestTasks.register(build)

    project.tasks.named('build').configure { Task task -> task.dependsOn('buildWorkspace') }
    project.tasks.named('check').configure { Task task -> task.dependsOn('fmtCheck', 'clippy') }
    project.tasks.named('clean').configure { Task task -> task.dependsOn('cleanSdk') }
  }
}
