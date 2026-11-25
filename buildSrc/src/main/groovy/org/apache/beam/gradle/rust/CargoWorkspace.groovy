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

/**
 * The crates of the Cargo workspace and the workspace crates that each one depends on.
 *
 * <p>Gradle uses this model for task inputs, so that a change to one crate runs only the
 * tasks of the crates that depend on it.
 */
@CompileStatic
class CargoWorkspace {

  /** A dependency of a crate on a workspace crate. */
  static class Dep {
    String alias
    String targetPkg
    boolean optional
    boolean defaultFeatures
    List<String> features
  }

  /** An entry of {@code [workspace.dependencies]} that has a path. */
  private static class WorkspaceDep {
    String pkg
    boolean defaultFeatures
    List<String> features
  }

  /** One entry of a Cargo.toml: the section and the entry on one line. */
  private static class TomlEntry {
    String section
    String line
  }

  /** The directory of each crate, by package name. */
  final Map<String, File> crateDir = new LinkedHashMap<>()
  /** The workspace crates that each crate depends on, the crate included. */
  final Map<String, Set<String>> crateClosure = new LinkedHashMap<>()
  /** The crates, each one after the crates that it depends on. */
  final List<String> topoOrder = []

  private final Map<String, String> dirToPkg = [:]
  private final Map<String, WorkspaceDep> workspaceDeps = [:]
  private final Map<String, Map<String, List<String>>> crateFeatures = [:]
  private final Map<String, List<Dep>> crateDeps = [:]
  private final Map<String, List<Dep>> crateDevDeps = [:]
  private final Map<String, List<String>> crateTestRequiredFeatures = [:]

  /**
   * @param memberTomls the Cargo.toml of each member crate, in a stable order
   * @param selectedPkg the crate of {@code -Ppkg}, or null
   * @param onlyFeatures the features of {@code -PonlyFeatures} for that crate, or null
   */
  CargoWorkspace(File rustDir, Collection<File> memberTomls, String selectedPkg, String onlyFeatures) {
    readWorkspaceDeps(new File(rustDir, 'Cargo.toml'))
    memberTomls.each { File toml -> readPackageName(toml) }
    crateDir.each { String pkg, File dir -> readManifest(pkg, dir) }
    crateDir.keySet().each { String pkg ->
      String features = (selectedPkg == pkg && onlyFeatures != null) ? onlyFeatures : null
      crateClosure[pkg] = new ClosureResolver(pkg, features).resolve()
    }
    Set<String> visited = new HashSet<>()
    crateDir.keySet().sort().each { String pkg -> visitTopo(pkg, visited) }
  }

  /** The library inputs of a crate. They exclude tests, benches and examples. */
  List<File> libInputFiles(String pkg) {
    File dir = crateDir[pkg]
    return [
      new File(dir, 'Cargo.toml'),
      new File(dir, 'build.rs'),
      new File(dir, 'src'),
      new File(dir, 'proto')
    ].findAll { File f -> f.exists() }
  }

  /** The test, bench and example directories of a crate. */
  List<File> testInputDirs(String pkg) {
    File dir = crateDir[pkg]
    return ['tests', 'benches', 'examples'].collect { String sub -> new File(dir, sub) }
        .findAll { File f -> f.exists() }
  }

  /** Reads a Cargo.toml: removes comments and joins each multiline value into one line. */
  private static List<TomlEntry> parseToml(File tomlFile) {
    List<TomlEntry> entries = []
    String section = ''
    StringBuilder pending = new StringBuilder()
    int depth = 0
    for (String raw : tomlFile.readLines('UTF-8')) {
      String clean = raw.replaceAll(/#.*$/, '').trim()
      if (clean.isEmpty()) {
        continue
      }
      if (depth == 0 && clean.startsWith('[') && clean.endsWith(']')) {
        section = clean.replaceAll(/^\[+|\]+$/, '').trim()
        continue
      }
      if (pending.length() > 0) {
        pending.append(' ')
      }
      pending.append(clean)
      for (int i = 0; i < clean.length(); i++) {
        int c = clean.charAt(i)
        if ('[{'.indexOf(c) >= 0) {
          depth++
        } else if (']}'.indexOf(c) >= 0) {
          depth = Math.max(0, depth - 1)
        }
      }
      if (depth == 0) {
        entries.add(new TomlEntry(section: section, line: pending.toString()))
        pending.setLength(0)
      }
    }
    return entries
  }

  private static List<String> quotedStrings(String text) {
    List<String> list = []
    Matcher m = (text =~ /"([^"]+)"/)
    while (m.find()) {
      list.add(m.group(1))
    }
    return list
  }

  private static List<String> featureArray(String rhs) {
    Matcher m = (rhs =~ /features\s*=\s*\[([^\]]*)\]/)
    return m.find() ? quotedStrings(m.group(1)) : new ArrayList<String>()
  }

  private static String firstGroup(String text, String pattern) {
    Matcher m = (text =~ pattern)
    return m.find() ? m.group(1) : null
  }

  private void readWorkspaceDeps(File rootToml) {
    parseToml(rootToml).each { TomlEntry entry ->
      if (entry.section != 'workspace.dependencies' || !entry.line.contains('path =')) {
        return
      }
      String[] kv = entry.line.split('=', 2)
      String alias = kv[0].trim()
      String rhs = kv[1].trim()
      if (firstGroup(rhs, /path\s*=\s*"([^"]+)"/) == null) {
        return
      }
      workspaceDeps[alias] = new WorkspaceDep(
          pkg: firstGroup(rhs, /package\s*=\s*"([^"]+)"/) ?: alias,
          defaultFeatures: !rhs.contains('default-features = false'),
          features: featureArray(rhs))
    }
  }

  private void readPackageName(File toml) {
    TomlEntry nameEntry = parseToml(toml).find { TomlEntry entry ->
      entry.section == 'package' && entry.line.startsWith('name')
    }
    String pkg = nameEntry == null ? null : firstGroup(nameEntry.line, /name\s*=\s*"([^"]+)"/)
    if (pkg != null) {
      crateDir[pkg] = toml.parentFile
      dirToPkg[toml.parentFile.canonicalPath] = pkg
    }
  }

  private void readManifest(String pkg, File dir) {
    crateFeatures[pkg] = [:]
    crateDeps[pkg] = []
    crateDevDeps[pkg] = []
    crateTestRequiredFeatures[pkg] = []
    parseToml(new File(dir, 'Cargo.toml')).each { TomlEntry entry ->
      String section = entry.section
      String line = entry.line
      if (section == 'features' && line.contains('=')) {
        String[] kv = line.split('=', 2)
        crateFeatures[pkg][kv[0].trim()] = quotedStrings(kv[1])
      } else if (section == 'dependencies' || section == 'build-dependencies') {
        Dep dep = parseDep(dir, line)
        if (dep != null) {
          crateDeps[pkg].add(dep)
        }
      } else if (section == 'dev-dependencies') {
        Dep dep = parseDep(dir, line)
        if (dep != null) {
          crateDevDeps[pkg].add(dep)
        }
      } else if ((section == 'test' || section == 'bench') && line.startsWith('required-features')) {
        crateTestRequiredFeatures[pkg].addAll(quotedStrings(line))
      }
    }
  }

  /** Returns the dependency of this line, or null if the target is not a workspace crate. */
  private Dep parseDep(File memberDir, String line) {
    if (!line.contains('=')) {
      return null
    }
    String[] kv = line.split('=', 2)
    String alias = kv[0].trim()
    String rhs = kv[1].trim()
    boolean fromWorkspace = rhs.contains('workspace = true')
    String targetPkg = null
    String packageName = firstGroup(rhs, /package\s*=\s*"([^"]+)"/)
    String path = firstGroup(rhs, /path\s*=\s*"([^"]+)"/)
    if (packageName != null && crateDir.containsKey(packageName)) {
      targetPkg = packageName
    } else if (path != null) {
      targetPkg = dirToPkg[new File(memberDir, path).canonicalPath]
    } else if (fromWorkspace && workspaceDeps.containsKey(alias)) {
      targetPkg = workspaceDeps[alias].pkg
    }
    if (targetPkg == null) {
      return null
    }
    WorkspaceDep shared = fromWorkspace ? workspaceDeps[alias] : null
    return new Dep(
        alias: alias,
        targetPkg: targetPkg,
        optional: rhs.contains('optional = true'),
        defaultFeatures: !rhs.contains('default-features = false') && (shared == null || shared.defaultFeatures),
        features: (featureArray(rhs) + (shared == null ? new ArrayList<String>() : shared.features)).unique())
  }

  private void visitTopo(String pkg, Set<String> visited) {
    if (!visited.add(pkg)) {
      return
    }
    crateClosure[pkg].each { String dep ->
      if (dep != pkg && crateDir.containsKey(dep)) {
        visitTopo(dep, visited)
      }
    }
    topoOrder.add(pkg)
  }

  /** Finds the workspace crates that one crate depends on, with the cargo feature rules. */
  private class ClosureResolver {
    private final String rootPkg
    private final String onlyFeatures
    private final Set<String> activeCrates = new LinkedHashSet<>()
    private final Set<String> enabledFeatures = new LinkedHashSet<>()
    private final Set<String> activatedEdges = new LinkedHashSet<>()

    ClosureResolver(String rootPkg, String onlyFeatures) {
      this.rootPkg = rootPkg
      this.onlyFeatures = onlyFeatures
    }

    Set<String> resolve() {
      activateCrate(rootPkg)
      return activeCrates
    }

    private void activateCrate(String pkg) {
      if (!activeCrates.add(pkg)) {
        return
      }
      depsOf(pkg).findAll { Dep d -> !d.optional }.each { Dep d -> activateEdge(pkg, d) }
      if (pkg != rootPkg) {
        return
      }
      (crateDevDeps[pkg] ?: new ArrayList<Dep>()).findAll { Dep d -> !d.optional }
          .each { Dep d -> activateEdge(pkg, d) }
      if (onlyFeatures != null) {
        onlyFeatures.split(',').collect { String f -> f.trim() }.findAll { String f -> !f.isEmpty() }
            .each { String f -> enableFeature(pkg, f) }
      } else {
        enableFeature(pkg, 'default')
        (crateTestRequiredFeatures[pkg] ?: new ArrayList<String>()).each { String f -> enableFeature(pkg, f) }
      }
    }

    private void activateEdge(String fromPkg, Dep d) {
      String edgeKey = "${fromPkg}->${d.alias}:${d.targetPkg}:${d.defaultFeatures}:${d.features.join(',')}"
      if (!activatedEdges.add(edgeKey)) {
        return
      }
      activateCrate(d.targetPkg)
      if (d.defaultFeatures) {
        enableFeature(d.targetPkg, 'default')
      }
      d.features.each { String f -> enableFeature(d.targetPkg, f) }
    }

    private void enableFeature(String pkg, String feature) {
      activateCrate(pkg)
      if (!enabledFeatures.add("${pkg}:${feature}".toString())) {
        return
      }
      depsOf(pkg).findAll { Dep d -> d.optional && d.alias == feature }.each { Dep d -> activateEdge(pkg, d) }
      List<String> tokens = (crateFeatures[pkg] ?: new HashMap<String, List<String>>())[feature]
      (tokens ?: new ArrayList<String>()).each { String token ->
        if (token.startsWith('dep:')) {
          String depAlias = token.substring(4)
          depsOf(pkg).findAll { Dep d -> d.alias == depAlias }.each { Dep d -> activateEdge(pkg, d) }
        } else if (token.contains('/')) {
          String[] parts = token.split('/', 2)
          String depAlias = parts[0].replace('?', '')
          depsOf(pkg).findAll { Dep d -> d.alias == depAlias }.each { Dep d ->
            if (!parts[0].endsWith('?')) {
              activateEdge(pkg, d)
            }
            enableFeature(d.targetPkg, parts[1])
          }
        } else {
          enableFeature(pkg, token)
        }
      }
    }

    private List<Dep> depsOf(String pkg) {
      return crateDeps[pkg] ?: new ArrayList<Dep>()
    }
  }
}
