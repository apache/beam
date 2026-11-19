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
import java.util.regex.Matcher
import org.gradle.api.GradleException
import org.gradle.api.Task
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskProvider

/**
 * The test tasks: one cacheable {@code test_<pkg>} task for each crate, {@code test} and
 * {@code testSummary}.
 *
 * <p>The inputs of {@code test_<pkg>} are the sources and tests of the crate, and the library
 * sources of the workspace crates that it depends on. A change to a test of one crate does
 * not run the tests of the crates that depend on it.
 */
@CompileStatic
class RustTestTasks {

  /** The test counts of one crate. */
  private static class Counts {
    int passed
    int failed
    int ignored
    int filtered
    String status
  }

  /** Writes to the console and keeps a copy, so that the task can count the results. */
  private static class TeeOutputStream extends OutputStream {
    final ByteArrayOutputStream captured = new ByteArrayOutputStream()

    @Override
    void write(int b) {
      System.out.write(b)
      captured.write(b)
    }

    @Override
    void write(byte[] b, int off, int len) {
      System.out.write(b, off, len)
      captured.write(b, off, len)
    }

    @Override
    void flush() {
      System.out.flush()
    }
  }

  static void register(RustSdkBuild build) {
    Map<String, Counts> results = new LinkedHashMap<>()
    Map<String, File> stampFiles = new LinkedHashMap<>()

    // Each test task runs after the previous one, so cargo processes do not wait on
    // target/.cargo-lock, and base crates run before the crates that depend on them.
    String previousTask = 'clippy'
    for (String pkg : build.workspace.topoOrder) {
      stampFiles[pkg] = stampFile(build, pkg)
      build.testTasks[pkg] = registerCrateTest(build, pkg, previousTask, stampFiles[pkg], results)
      previousTask = "test_${pkg}".toString()
    }
    registerSummary(build, results, stampFiles)
    registerTest(build)
  }

  private static File stampFile(RustSdkBuild build, String pkg) {
    boolean ownFeatures = build.selectedPkg == pkg && build.onlyFeatures != null
    String featureSuffix = ownFeatures
        ? "-features-${build.onlyFeatures.replaceAll('[^a-zA-Z0-9_-]', '_')}" : ''
    String filterSuffix = build.testArgs ? '-filtered' : ''
    return build.buildFile("test-results/cargo/${pkg}/test${featureSuffix}${filterSuffix}.txt")
  }

  private static TaskProvider<Exec> registerCrateTest(
      RustSdkBuild build, String pkg, String runAfter, File stampFile, Map<String, Counts> results) {
    List<String> testArgs = build.testArgs
    List<String> featureScope = build.selectedPkg == pkg ? build.featureScope : new ArrayList<String>()
    Set<String> closure = build.workspace.crateClosure[pkg]

    return build.project.tasks.register("test_${pkg}".toString(), Exec) { Exec task ->
      task.group = 'Verification'
      task.description = "Run cargo test for ${pkg}"
      task.workingDir(build.rustDir)
      task.commandLine(['cargo', 'test', '-p', pkg] + featureScope + build.noFailFast +
          (testArgs ? ['--'] + testArgs : new ArrayList<String>()))
      task.ignoreExitValue = true
      task.finalizedBy('testSummary')
      task.mustRunAfter(runAfter, 'buildWorkspace')

      task.inputs.property('pkg', pkg)
      task.inputs.property('beamSdkVersion', build.versions.sdkVersion)
      task.inputs.property('beamReleaseVersion', build.versions.releaseVersion)
      task.inputs.property('featureScope', featureScope)
      task.inputs.property('testArgs', testArgs)
      task.inputs.files(build.workspaceRootFiles).withPathSensitivity(PathSensitivity.RELATIVE)
      if ('apache-beam-model' in closure) {
        task.inputs.files(build.modelProtoDirs).withPathSensitivity(PathSensitivity.RELATIVE)
      }
      closure.each { String dep ->
        task.inputs.files(build.workspace.libInputFiles(dep)).withPathSensitivity(PathSensitivity.RELATIVE)
      }
      task.inputs.files(build.testInputFiles(pkg)).withPathSensitivity(PathSensitivity.RELATIVE)

      task.outputs.file(stampFile)
      task.outputs.upToDateWhen { testArgs.isEmpty() && readStamp(stampFile) != null }
      task.outputs.cacheIf { testArgs.isEmpty() }

      TeeOutputStream output = new TeeOutputStream()
      task.doFirst {
        output.captured.reset()
        task.standardOutput = output
      }
      task.doLast {
        Counts counts = countResults(output.captured.toString('UTF-8'), testArgs.isEmpty())
        task.logger.lifecycle("Total for ${pkg}: ${counts.passed} passed; ${counts.failed} failed; " +
            "${counts.ignored} ignored; ${counts.filtered} filtered out")
        int exitCode = task.executionResult.get().exitValue
        counts.status = exitCode == 0 ? 'EXECUTED' : 'FAILED'
        results[pkg] = counts
        if (exitCode != 0) {
          stampFile.delete()
          task.executionResult.get().assertNormalExitValue()
        }
        RustSdkBuild.writeStamp(stampFile, "passed=${counts.passed} failed=${counts.failed} " +
            "ignored=${counts.ignored} filtered=${counts.filtered}\n")
      }
    }
  }

  /** Adds the {@code test result:} lines of the cargo output. */
  private static Counts countResults(String output, boolean noFilter) {
    Counts total = new Counts()
    Matcher m = (output =~
        /test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out/)
    while (m.find()) {
      int filtered = m.group(4) as int
      // A test that runs its own binary with `--exact` prints a second line with a filter.
      if (noFilter && filtered > 0) {
        continue
      }
      total.passed += m.group(1) as int
      total.failed += m.group(2) as int
      total.ignored += m.group(3) as int
      total.filtered += filtered
    }
    return total
  }

  /** Reads the counts of a stamp file, or null if the file has none. */
  private static Counts readStamp(File stampFile) {
    if (!stampFile.isFile()) {
      return null
    }
    Matcher m = (stampFile.text =~ /passed=(\d+)\s+failed=(\d+)\s+ignored=(\d+)\s+filtered=(\d+)/)
    if (!m.find()) {
      return null
    }
    return new Counts(
        passed: m.group(1) as int,
        failed: m.group(2) as int,
        ignored: m.group(3) as int,
        filtered: m.group(4) as int)
  }

  private static void registerSummary(
      RustSdkBuild build, Map<String, Counts> results, Map<String, File> stampFiles) {
    build.project.tasks.register('testSummary') { Task task ->
      task.group = 'Verification'
      task.description = 'Print the aggregated per-crate test summary'
      task.mustRunAfter(build.testTasks.values())
      task.doLast {
        List<String> packages = build.selectedPkg != null ? [build.selectedPkg] : build.defaultPackages()
        Map<String, Counts> rows = new LinkedHashMap<>()
        packages.each { String pkg ->
          Counts counts = results[pkg]
          if (counts == null && stampFiles[pkg] != null) {
            counts = readStamp(stampFiles[pkg])
            if (counts != null) {
              counts.status = 'UP-TO-DATE'
            }
          }
          if (counts != null) {
            rows[pkg] = counts
          }
        }
        printSummary(task.logger, rows)
      }
    }
  }

  private static void printSummary(Logger logger, Map<String, Counts> rows) {
    if (rows.isEmpty()) {
      return
    }
    int nameWidth = Math.max(28, rows.keySet().collect { String pkg -> pkg.length() }.max() + 2)
    String separator = '-' * (nameWidth + 46)
    String rowFormat = "%-${nameWidth}s %8d %8d %9d %10d"
    logger.lifecycle(separator)
    logger.lifecycle(String.format("%-${nameWidth}s %8s %8s %9s %10s   %s",
        'Crate', 'Passed', 'Failed', 'Ignored', 'Filtered', 'Status'))
    logger.lifecycle(separator)
    rows.each { String pkg, Counts c ->
      logger.lifecycle(String.format("${rowFormat}   %s", pkg, c.passed, c.failed, c.ignored, c.filtered, c.status))
    }
    logger.lifecycle(separator)
    logger.lifecycle(String.format(rowFormat,
        "Total (${rows.size()} ${rows.size() == 1 ? 'crate' : 'crates'})".toString(),
        rows.values().sum { Counts c -> c.passed },
        rows.values().sum { Counts c -> c.failed },
        rows.values().sum { Counts c -> c.ignored },
        rows.values().sum { Counts c -> c.filtered }))
    logger.lifecycle(separator)
  }

  private static void registerTest(RustSdkBuild build) {
    build.project.tasks.register('test') { Task task ->
      task.group = 'Verification'
      task.description = 'Run tests (the workspace, or one crate with -Ppkg=<crate>; ' +
          '-PtestArgs=<args> after --; --continue runs every test binary)'
      task.finalizedBy('testSummary')
      String pkg = build.selectedPkg
      if (pkg == null) {
        task.dependsOn(build.project.provider {
          build.defaultPackages().collect { String name -> build.testTasks[name] }
        })
      } else if (build.testTasks.containsKey(pkg)) {
        task.dependsOn(build.testTasks[pkg])
      } else {
        throw new GradleException("Unknown Rust workspace package '${pkg}'. " +
            "Available packages: ${build.workspace.topoOrder.join(', ')}")
      }
    }
  }
}
