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
package org.apache.beam.runners.prism;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * Locates a Prism executable based on a user's default operating system and architecture
 * environment or a {@link PrismPipelineOptions#getPrismLocation()} override. Handles the download,
 * unzip, {@link PosixFilePermissions}, as needed. For {@link #GITHUB_DOWNLOAD_PREFIX} sources,
 * additionally performs a SHA512 verification.
 */
class PrismLocator {
  static final String OS_NAME_PROPERTY = "os.name";
  static final String ARCH_PROPERTY = "os.arch";
  static final String USER_HOME_PROPERTY = "user.home";

  private static final String ZIP_EXT = "zip";
  private static final ReleaseInfo RELEASE_INFO = ReleaseInfo.getReleaseInfo();
  private static final String PRISM_BIN_PATH = ".apache_beam/cache/prism/bin";
  private static final Set<PosixFilePermission> PERMS =
      PosixFilePermissions.fromString("rwxr-xr-x");
  private static final String GITHUB_COMMON_PREFIX = "https://github.com/apache/beam/releases/";
  private static final String GITHUB_DOWNLOAD_PREFIX = GITHUB_COMMON_PREFIX + "download";
  private static final String GITHUB_TAG_PREFIX = GITHUB_COMMON_PREFIX + "tag";

  private final PrismPipelineOptions options;

  PrismLocator(PrismPipelineOptions options) {
    this.options = options;
  }

  String resolveLocation() {
    String from =
        String.format(
            "%s/v%s/%s.zip", GITHUB_DOWNLOAD_PREFIX, RELEASE_INFO.getSdkVersion(), buildFileName());

    if (Strings.isNullOrEmpty(options.getPrismLocation())) {
      return from;
    }
    from = options.getPrismLocation();

    // Likely a local file, return it directly.
    if (!from.startsWith("http")) {
      return from;
    }

    // Validate that it's from a safe location: A Beam Github Release
    checkArgument(
        options.getPrismLocation().startsWith(GITHUB_COMMON_PREFIX),
        "Provided --prismLocation URL is not an Apache Beam Github "
            + "Release page URL or download URL: ",
        options.getPrismLocation());

    from = options.getPrismLocation();

    // If this is the tag prefix, then build the release download with the version
    // from the given url.
    if (options.getPrismLocation().startsWith(GITHUB_TAG_PREFIX)) {
      Path tagPath = Paths.get(options.getPrismLocation());
      Path locVersion = tagPath.getName(tagPath.getNameCount() - 1);
      // The "v" prefix is already included in the version name segment.
      from = String.format("%s/%s/%s.zip", GITHUB_DOWNLOAD_PREFIX, locVersion, buildFileName());
    }
    checkArgument(
        from.startsWith(GITHUB_DOWNLOAD_PREFIX),
        "Provided --prismLocation URL could not be resolved to a download URL. ",
        options.getPrismLocation());
    return from;
  }

  /**
   * Downloads and prepares a Prism executable for use with the {@link PrismRunner}. The returned
   * {@link String} is the absolute path to the Prism executable.
   */
  String resolve() throws IOException {
    String from = resolveLocation();

    String fromFileName = getNameWithoutExtension(from);
    Path to = Paths.get(userHome(), PRISM_BIN_PATH, fromFileName);

    if (Files.exists(to)) {
      return to.toString();
    }

    createDirectoryIfNeeded(to);

    if (from.startsWith("http")) {
      String result = resolve(new URL(from), to);
      checkState(Files.exists(to), "Resolved location does not exist: %s", result);
      return result;
    }

    String result = resolve(Paths.get(from), to);
    checkState(Files.exists(to), "Resolved location does not exist: %s", result);
    return result;
  }

  static Path prismBinDirectory() {
    return Paths.get(userHome(), PRISM_BIN_PATH);
  }

  private String resolve(URL from, Path to) throws IOException {
    BiConsumer<URL, Path> downloadFn = PrismLocator::download;
    if (from.getPath().endsWith(ZIP_EXT)) {
      downloadFn = PrismLocator::unzip;
    }
    downloadFn.accept(from, to);

    Files.setPosixFilePermissions(to, PERMS);

    return to.toString();
  }

  private String resolve(Path from, Path to) throws IOException {

    BiConsumer<InputStream, Path> copyFn = PrismLocator::copy;
    if (from.endsWith(ZIP_EXT)) {
      copyFn = PrismLocator::unzip;
    }

    copyFn.accept(from.toUri().toURL().openStream(), to);
    try (OutputStream out = Files.newOutputStream(to)) {
      ByteStreams.copy(from.toUri().toURL().openStream(), out);
    }
    Files.setPosixFilePermissions(to, PERMS);

    return to.toString();
  }

  private static void unzip(URL from, Path to) {
    try {
      unzip(from.openStream(), to);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void unzip(InputStream from, Path to) {
    try (OutputStream out = Files.newOutputStream(to)) {
      ZipInputStream zis = new ZipInputStream(from);
      for (ZipEntry entry = zis.getNextEntry(); entry != null; entry = zis.getNextEntry()) {
        InputStream in = ByteStreams.limit(zis, entry.getSize());
        ByteStreams.copy(in, out);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void copy(InputStream from, Path to) {
    try (OutputStream out = Files.newOutputStream(to)) {
      ByteStreams.copy(from, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void download(URL from, Path to) {
    try (OutputStream out = Files.newOutputStream(to)) {
      ByteStreams.copy(from.openStream(), out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getNameWithoutExtension(String path) {
    return org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files
        .getNameWithoutExtension(path);
  }

  private String buildFileName() {
    String version = getSDKVersion();
    return String.format("apache_beam-v%s-prism-%s-%s", version, os(), arch());
  }

  private String getSDKVersion() {
    if (Strings.isNullOrEmpty(options.getPrismVersionOverride())) {
      return RELEASE_INFO.getSdkVersion();
    }
    return options.getPrismVersionOverride();
  }

  private static String os() {
    String result = mustGetPropertyAsLowerCase(OS_NAME_PROPERTY);
    if (result.contains("mac")) {
      return "darwin";
    }
    return result;
  }

  private static String arch() {
    String result = mustGetPropertyAsLowerCase(ARCH_PROPERTY);
    if (result.contains("aarch")) {
      return "arm64";
    }
    return result;
  }

  private static String userHome() {
    return mustGetPropertyAsLowerCase(USER_HOME_PROPERTY);
  }

  private static String mustGetPropertyAsLowerCase(String name) {
    return checkStateNotNull(System.getProperty(name), "System property: " + name + " not set")
        .toLowerCase();
  }

  private static void createDirectoryIfNeeded(Path path) throws IOException {
    Path parent = path.getParent();
    if (parent == null) {
      return;
    }
    Files.createDirectories(parent);
  }
}
