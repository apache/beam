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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files.fileTraverser;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for local files.
 *
 * <p>{@link #match} should interpret {@code spec} and resolve paths correctly according to OS being
 * used. In order to do that specs should be defined in one of the below formats:
 *
 * <p>Linux/Mac:
 *
 * <ul>
 *   <li>pom.xml
 *   <li>/Users/beam/Documents/pom.xml
 *   <li>file:/Users/beam/Documents/pom.xml
 *   <li>file:///Users/beam/Documents/pom.xml
 * </ul>
 *
 * <p>Windows OS:
 *
 * <ul>
 *   <li>pom.xml
 *   <li>C:/Users/beam/Documents/pom.xml
 *   <li>C:\\Users\\beam\\Documents\\pom.xml
 *   <li>file:/C:/Users/beam/Documents/pom.xml
 *   <li>file:///C:/Users/beam/Documents/pom.xml
 * </ul>
 */
class LocalFileSystem extends FileSystem<LocalResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystem.class);

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  LocalFileSystem() {}

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    return match(new File(".").getAbsolutePath(), specs);
  }

  @VisibleForTesting
  List<MatchResult> match(String baseDir, List<String> specs) throws IOException {
    ImmutableList.Builder<MatchResult> ret = ImmutableList.builder();
    for (String spec : specs) {
      ret.add(matchOne(baseDir, spec));
    }
    return ret.build();
  }

  @Override
  protected WritableByteChannel create(LocalResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    LOG.debug("creating file {}", resourceId);
    File absoluteFile = resourceId.getPath().toFile().getAbsoluteFile();
    if (absoluteFile.getParentFile() != null
        && !absoluteFile.getParentFile().exists()
        && !absoluteFile.getParentFile().mkdirs()
        && !absoluteFile.getParentFile().exists()) {
      throw new IOException("Unable to create parent directories for '" + resourceId + "'");
    }
    return Channels.newChannel(new BufferedOutputStream(new FileOutputStream(absoluteFile)));
  }

  @Override
  protected ReadableByteChannel open(LocalResourceId resourceId) throws IOException {
    LOG.debug("opening file {}", resourceId);
    @SuppressWarnings("resource") // The caller is responsible for closing the channel.
    FileInputStream inputStream = new FileInputStream(resourceId.getPath().toFile());
    // Use this method for creating the channel (rather than new FileChannel) so that we get
    // regular FileNotFoundException. Closing the underyling channel will close the inputStream.
    return inputStream.getChannel();
  }

  @Override
  protected void copy(List<LocalResourceId> srcResourceIds, List<LocalResourceId> destResourceIds)
      throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    int numFiles = srcResourceIds.size();
    for (int i = 0; i < numFiles; i++) {
      LocalResourceId src = srcResourceIds.get(i);
      LocalResourceId dst = destResourceIds.get(i);
      LOG.debug("Copying {} to {}", src, dst);
      File parent = dst.getCurrentDirectory().getPath().toFile();
      if (!parent.exists()) {
        checkArgument(
            parent.mkdirs() || parent.exists(),
            "Unable to make output directory %s in order to copy into file %s",
            parent,
            dst.getPath());
      }
      // Copy the source file, replacing the existing destination.
      // Paths.get(x) will not work on Windows OSes cause of the ":" after the drive letter.
      Files.copy(
          src.getPath(),
          dst.getPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  @Override
  protected void rename(List<LocalResourceId> srcResourceIds, List<LocalResourceId> destResourceIds)
      throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    int numFiles = srcResourceIds.size();
    for (int i = 0; i < numFiles; i++) {
      LocalResourceId src = srcResourceIds.get(i);
      LocalResourceId dst = destResourceIds.get(i);
      LOG.debug("Renaming {} to {}", src, dst);
      File parent = dst.getCurrentDirectory().getPath().toFile();
      if (!parent.exists()) {
        checkArgument(
            parent.mkdirs() || parent.exists(),
            "Unable to make output directory %s in order to move into file %s",
            parent,
            dst.getPath());
      }
      // Rename the source file, replacing the existing destination.
      Files.move(
          src.getPath(),
          dst.getPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    }
  }

  @Override
  protected void delete(Collection<LocalResourceId> resourceIds) throws IOException {
    for (LocalResourceId resourceId : resourceIds) {
      try {
        Files.delete(resourceId.getPath());
      } catch (NoSuchFileException e) {
        LOG.info(
            "Ignoring failed deletion of file {} which already does not exist: {}", resourceId, e);
      }
    }
  }

  @Override
  protected LocalResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    Path path = Paths.get(singleResourceSpec);
    return LocalResourceId.fromPath(path, isDirectory);
  }

  @Override
  protected String getScheme() {
    return "file";
  }

  private MatchResult matchOne(String baseDir, String spec) {
    if (spec.toLowerCase().startsWith("file:")) {
      spec = spec.substring("file:".length());
    }

    if (SystemUtils.IS_OS_WINDOWS) {
      List<String> prefixes = Arrays.asList("///", "/");
      for (String prefix : prefixes) {
        if (spec.toLowerCase().startsWith(prefix)) {
          spec = spec.substring(prefix.length());
        }
      }
    }

    // BEAM-6213: Windows breaks on Paths.get(spec).toFile() with a glob because
    // it considers it an invalid file system pattern. We should use
    // new File(spec) to avoid such validation.
    // See https://bugs.openjdk.java.net/browse/JDK-8197918
    // However, new File(parent, child) resolves absolute `child` in a system-dependent
    // way that is generally incorrect, for example new File($PWD, "/tmp/foo") resolves
    // to $PWD/tmp/foo on many systems, unlike Paths.get($PWD).resolve("/tmp/foo") which
    // correctly resolves to "/tmp/foo". We add just this one piece of logic here, without
    // switching to Paths which could require a rewrite of this module to support
    // both Windows and correct file resolution.
    // The root cause is that globs are not files but we are using file manipulation libraries
    // to work with them.
    final File specAsFile = new File(spec);
    final File absoluteFile = specAsFile.isAbsolute() ? specAsFile : new File(baseDir, spec);

    if (absoluteFile.exists()) {
      return MatchResult.create(Status.OK, ImmutableList.of(toMetadata(absoluteFile)));
    }

    File parent = getSpecNonGlobPrefixParentFile(absoluteFile.getAbsolutePath());
    if (!parent.exists()) {
      return MatchResult.create(Status.NOT_FOUND, Collections.emptyList());
    }

    // Method getAbsolutePath() on Windows platform may return something like
    // "c:\temp\file.txt". FileSystem.getPathMatcher() call below will treat
    // '\' (backslash) as an escape character, instead of a directory
    // separator. Replacing backslash with double-backslash solves the problem.
    // We perform the replacement on all platforms, even those that allow
    // backslash as a part of the filename, because Globs.toRegexPattern will
    // eat one backslash.
    String pathToMatch =
        absoluteFile
            .getAbsolutePath()
            .replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("\\\\"));

    final PathMatcher matcher =
        java.nio.file.FileSystems.getDefault().getPathMatcher("glob:" + pathToMatch);

    // TODO: Avoid iterating all files: https://issues.apache.org/jira/browse/BEAM-1309
    Iterable<File> files = fileTraverser().depthFirstPreOrder(parent);
    Iterable<File> matchedFiles =
        StreamSupport.stream(files.spliterator(), false)
            .filter(
                Predicates.and(
                        org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files.isFile(),
                        input -> matcher.matches(input.toPath()))
                    ::apply)
            .collect(Collectors.toList());

    List<Metadata> result = Lists.newLinkedList();
    for (File match : matchedFiles) {
      result.add(toMetadata(match));
    }
    if (result.isEmpty()) {
      // TODO: consider to return Status.OK for globs.
      return MatchResult.create(
          Status.NOT_FOUND,
          new FileNotFoundException(
              String.format("No files found for spec: %s in working directory %s", spec, baseDir)));
    } else {
      return MatchResult.create(Status.OK, result);
    }
  }

  private File getSpecNonGlobPrefixParentFile(String spec) {
    String specNonWildcardPrefix = getNonWildcardPrefix(spec);
    File file = new File(specNonWildcardPrefix);
    return specNonWildcardPrefix.endsWith(File.separator)
        ? file.getAbsoluteFile()
        : file.getAbsoluteFile().getParentFile();
  }

  private Metadata toMetadata(File file) {
    return Metadata.builder()
        .setResourceId(LocalResourceId.fromPath(file.toPath(), file.isDirectory()))
        .setIsReadSeekEfficient(true)
        .setSizeBytes(file.length())
        .setLastModifiedMillis(file.lastModified())
        .build();
  }

  private static String getNonWildcardPrefix(String globExp) {
    Matcher m = GLOB_PREFIX.matcher(globExp);
    return !m.matches() ? globExp : m.group("PREFIX");
  }
}
