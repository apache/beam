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
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.TaskProvider

/** The settings that the task groups of the Rust SDK build share. */
@CompileStatic
class RustSdkBuild {
  final Project project
  final RustSdkExtension extension
  final BeamVersions versions
  /** The root of the Cargo workspace. */
  final File rustDir
  final CargoWorkspace workspace

  /** The crate of {@code -Ppkg=<crate>}, or null for the workspace. */
  final String selectedPkg
  /** With {@code -Ppkg}, the only cargo features of {@code -PonlyFeatures=<list>}, or null. */
  final String onlyFeatures
  final List<String> workspaceScope = ['--workspace']
  final List<String> featureScope
  /** The cargo arguments that select the crate of {@code -Ppkg}, or the workspace. */
  final List<String> cargoScope
  /** The arguments of {@code -PtestArgs="<args>"}, which go to the test binary after {@code --}. */
  final List<String> testArgs
  /** With {@code --continue}, cargo runs each test binary and reports all failures. */
  final List<String> noFailFast

  final List<File> modelProtoDirs
  final List<File> workspaceRootFiles
  /** The {@code test_<pkg>} task of each crate. */
  final Map<String, TaskProvider<Exec>> testTasks = new LinkedHashMap<>()

  RustSdkBuild(Project project, RustSdkExtension extension) {
    this.project = project
    this.extension = extension
    versions = new BeamVersions(project)
    rustDir = new File(project.rootDir, 'sdks/rust')

    selectedPkg = (project.findProperty('pkg') ?: project.findProperty('package'))?.toString()
    onlyFeatures = project.findProperty('onlyFeatures')?.toString()
    featureScope = (selectedPkg != null && onlyFeatures != null)
        ? ['--no-default-features', '--features', onlyFeatures] : new ArrayList<String>()
    cargoScope = (selectedPkg != null ? ['-p', selectedPkg] : workspaceScope) + featureScope
    testArgs = project.findProperty('testArgs')?.toString()?.tokenize() ?: new ArrayList<String>()
    noFailFast = project.gradle.startParameter.continueOnFailure
        ? ['--no-fail-fast'] : new ArrayList<String>()

    ConfigurableFileTree memberTomls = project.fileTree(rustDir)
    memberTomls.include('**/Cargo.toml')
    memberTomls.exclude('Cargo.toml', 'target/**', 'container/**', 'build/**', 'examples/build/**')
    workspace = new CargoWorkspace(
        rustDir, memberTomls.files.sort { File f -> f.absolutePath }, selectedPkg, onlyFeatures)

    modelProtoDirs = [
      'model/pipeline/src/main/proto',
      'model/fn-execution/src/main/proto',
      'model/job-management/src/main/proto',
    ].collect { String path -> new File(project.rootDir, path) }
    workspaceRootFiles = [
      new File(rustDir, 'Cargo.toml'),
      new File(rustDir, 'Cargo.lock'),
      new File(project.rootDir, 'gradle.properties'),
    ]
  }

  /** Registers an Exec task that runs {@code cargo <args>} in the workspace root. */
  TaskProvider<Exec> cargoTask(String name, String taskGroup, String text, List<String> args) {
    return project.tasks.register(name, Exec) { Exec task ->
      task.group = taskGroup
      task.description = text
      task.workingDir(rustDir)
      task.commandLine(['cargo'] + args)
    }
  }

  /** The crates of the default {@code test}: all but the benchmark crates. */
  List<String> defaultPackages() {
    List<String> benchmarks = extension.benchmarkPackages.get()
    return workspace.topoOrder.findAll { String pkg -> !(pkg in benchmarks) }
  }

  /** The test, bench and example files of a crate, without the Python suites in them. */
  List<ConfigurableFileTree> testInputFiles(String pkg) {
    return workspace.testInputDirs(pkg).collect { File dir ->
      ConfigurableFileTree tree = project.fileTree(dir)
      tree.exclude('python/**', '**/__pycache__/**', '**/.pytest_cache/**')
      return tree
    }
  }

  /** The library and test inputs of these crates. */
  List<Object> libAndTestInputs(Collection<String> packages) {
    List<Object> inputs = []
    packages.each { String pkg ->
      inputs.addAll(workspace.libInputFiles(pkg))
      inputs.addAll(testInputFiles(pkg))
    }
    return inputs
  }

  /** A file in the build directory of the project. */
  File buildFile(String path) {
    return new File(project.layout.buildDirectory.get().asFile, path)
  }

  /** Writes the stamp file that marks a task with no other output as done. */
  static void writeStamp(File stampFile, String text = 'ok\n') {
    stampFile.parentFile.mkdirs()
    stampFile.text = text
  }
}
