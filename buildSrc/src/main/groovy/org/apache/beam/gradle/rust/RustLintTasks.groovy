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
import org.gradle.api.Action
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.process.ExecSpec

/** The build, format, lint and clean tasks. */
@CompileStatic
class RustLintTasks {

  /** The lints that test and benchmark code can break. clippy.toml exempts {@code #[cfg(test)]}. */
  private static final List<String> TEST_CLIPPY_ALLOWS = [
    'unwrap_used',
    'expect_used',
    'panic',
    'missing_panics_doc',
  ].collectMany { String lint -> ['-A', "clippy::${lint}".toString()] }

  static void register(RustSdkBuild build) {
    registerBuildWorkspace(build)
    registerFmt(build)
    registerClippy(build)
    build.cargoTask('cleanSdk', 'Build', 'Remove the cargo target directory', ['clean'])
  }

  private static void registerBuildWorkspace(RustSdkBuild build) {
    build.cargoTask('buildWorkspace', 'Build',
        'Compile all crates in the Rust workspace but the benchmarks (or one crate with -Ppkg=<crate>)',
        ['build'] + build.cargoScope).configure { Exec task ->
          String pkg = build.selectedPkg
          CargoWorkspace workspace = build.workspace
          task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
          task.inputs.property('cargoScope', build.cargoScope)
          task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.project.provider {
            if (pkg != null && workspace.crateClosure.containsKey(pkg)) {
              List<Object> inputs = []
              workspace.crateClosure[pkg].each { String dep -> inputs.addAll(workspace.libInputFiles(dep)) }
              inputs.addAll(build.testInputFiles(pkg))
              return inputs
            }
            return build.libAndTestInputs(build.defaultPackages())
          }).withPathSensitivity(PathSensitivity.RELATIVE)
          String buildKey = pkg != null
              ? "${pkg}-${build.onlyFeatures ?: 'default'}".replaceAll('[^a-zA-Z0-9_-]', '_')
              : 'workspace'
          File stampFile = build.buildFile("test-results/cargo/build-${buildKey}.txt")
          task.outputs.file(stampFile)
          // Cargo decides what to compile; Gradle only skips the call when no input changed.
          task.outputs.upToDateWhen { new File(build.rustDir, 'target').exists() }
          task.doLast { RustSdkBuild.writeStamp(stampFile) }
        }
  }

  private static void registerFmt(RustSdkBuild build) {
    build.cargoTask('fmtCheck', 'Verification', 'Verify formatting (cargo fmt --check)',
        ['fmt', '--check']).configure { Exec task ->
          task.inputs.files(new File(build.rustDir, 'rustfmt.toml'), new File(build.rustDir, 'Cargo.toml'))
              .withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.libAndTestInputs(build.workspace.topoOrder))
              .withPathSensitivity(PathSensitivity.RELATIVE)
          File stampFile = build.buildFile('test-results/cargo/fmtCheck.txt')
          task.outputs.file(stampFile)
          task.outputs.cacheIf { true }
          task.doLast { RustSdkBuild.writeStamp(stampFile) }
        }
    build.cargoTask('fmt', 'Verification', 'Format all Rust code (cargo fmt)', ['fmt'])
  }

  private static void registerClippy(RustSdkBuild build) {
    build.cargoTask('clippy', 'Verification', 'Run clippy on all targets (-D warnings)',
        ['clippy'] + build.workspaceScope + ['--lib', '--bins', '--examples', '--', '-D', 'warnings'])
        .configure { Exec task ->
          task.mustRunAfter('fmtCheck')
          task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
          task.inputs.files(build.workspaceRootFiles + [new File(build.rustDir, 'clippy.toml')])
              .withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
          task.inputs.files(build.project.provider { build.libAndTestInputs(build.defaultPackages()) })
              .withPathSensitivity(PathSensitivity.RELATIVE)
          File stampFile = build.buildFile('test-results/cargo/clippy.txt')
          task.outputs.file(stampFile)
          task.outputs.cacheIf { true }
          task.doLast {
            build.project.exec({ ExecSpec spec ->
              spec.workingDir(build.rustDir)
              spec.environment(build.versions.cargoEnv())
              spec.commandLine(['cargo', 'clippy'] + build.workspaceScope +
                  ['--tests', '--benches', '--', '-D', 'warnings'] + TEST_CLIPPY_ALLOWS)
            } as Action<ExecSpec>)
            RustSdkBuild.writeStamp(stampFile)
          }
        }
  }
}
