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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import javax.annotation.Nullable;

import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@link IOChannelFactory} for local files.
 */
public class FileIOChannelFactory implements IOChannelFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileIOChannelFactory.class);

   /**
   * Create a {@link FileIOChannelFactory} with the given {@link PipelineOptions}.
   */
  public static FileIOChannelFactory fromOptions(@Nullable PipelineOptions options) {
    return new FileIOChannelFactory();
  }

  private FileIOChannelFactory() {}

  /**
   *  Converts the given file spec to a java {@link File}. If {@code spec} is actually a URI with
   *  the {@code file} scheme, then this function will ensure that the returned {@link File}
   *  has the correct path.
   */
  private static File specToFile(String spec) {
    try {
      // Handle URI.
      URI uri = URI.create(spec);
      return Paths.get(uri).toFile();
    } catch (IllegalArgumentException e) {
      // Fall back to assuming this is actually a file.
      return Paths.get(spec).toFile();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Wildcards in the directory portion are not supported.
   */
  @Override
  public Collection<String> match(String spec) throws IOException {
    File file = specToFile(spec);

    File parent = file.getAbsoluteFile().getParentFile();
    if (!parent.exists()) {
      return Collections.EMPTY_LIST;
    }

    // Method getAbsolutePath() on Windows platform may return something like
    // "c:\temp\file.txt". FileSystem.getPathMatcher() call below will treat
    // '\' (backslash) as an escape character, instead of a directory
    // separator. Replacing backslash with double-backslash solves the problem.
    // We perform the replacement on all platforms, even those that allow
    // backslash as a part of the filename, because Globs.toRegexPattern will
    // eat one backslash.
    String pathToMatch = file.getAbsolutePath().replaceAll(Matcher.quoteReplacement("\\"),
                                                           Matcher.quoteReplacement("\\\\"));

    final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathToMatch);

    Iterable<File> files = com.google.common.io.Files.fileTreeTraverser().preOrderTraversal(parent);
    Iterable<File> matchedFiles = Iterables.filter(files,
        Predicates.and(
            com.google.common.io.Files.isFile(),
            new Predicate<File>() {
              @Override
              public boolean apply(File input) {
                return matcher.matches(input.toPath());
              }
        }));

    List<String> result = new LinkedList<>();
    for (File match : matchedFiles) {
      result.add(match.getPath());
    }

    return result;
  }

  @Override
  public ReadableByteChannel open(String spec) throws IOException {
    LOG.debug("opening file {}", spec);
    @SuppressWarnings("resource") // The caller is responsible for closing the channel.
    FileInputStream inputStream = new FileInputStream(specToFile(spec));
    // Use this method for creating the channel (rather than new FileChannel) so that we get
    // regular FileNotFoundException. Closing the underyling channel will close the inputStream.
    return inputStream.getChannel();
  }

  @Override
  public WritableByteChannel create(String spec, String mimeType)
      throws IOException {
    LOG.debug("creating file {}", spec);
    File file = specToFile(spec);
    if (file.getAbsoluteFile().getParentFile() != null
        && !file.getAbsoluteFile().getParentFile().exists()
        && !file.getAbsoluteFile().getParentFile().mkdirs()
        && !file.getAbsoluteFile().getParentFile().exists()) {
      throw new IOException("Unable to create parent directories for '" + spec + "'");
    }
    return Channels.newChannel(
        new BufferedOutputStream(new FileOutputStream(file)));
  }

  @Override
  public long getSizeBytes(String spec) throws IOException {
    try {
      return Files.size(specToFile(spec).toPath());
    } catch (NoSuchFileException e) {
      throw new FileNotFoundException(e.getReason());
    }
  }

  @Override
  public boolean isReadSeekEfficient(String spec) throws IOException {
    return true;
  }

  @Override
  public String resolve(String path, String other) throws IOException {
    return toPath(path).resolve(other).toString();
  }

  @Override
  public Path toPath(String path) {
    return specToFile(path).toPath();
  }

  @Override
  public void copy(Iterable<String> srcFilenames, Iterable<String> destFilenames) throws
      IOException {
    List<String> srcList = Lists.newArrayList(srcFilenames);
    List<String> destList = Lists.newArrayList(destFilenames);
    checkArgument(
        srcList.size() == destList.size(),
        "Number of source files %s must equal number of destination files %s",
        srcList.size(),
        destList.size());
    int numFiles = srcList.size();
    for (int i = 0; i < numFiles; i++) {
      String src = srcList.get(i);
      String dst = destList.get(i);
      LOG.info("Copying {} to {}", src, dst);
      try {
        // Copy the source file, replacing the existing destination.
        // Paths.get(x) will not work on Windows OSes cause of the ":" after the drive letter.
        Files.copy(
            new File(src).toPath(),
            new File(dst).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      } catch (NoSuchFileException e) {
        LOG.info("{} does not exist.", src);
        // Suppress exception if file does not exist.
      }
    }
  }

  @Override
  public void remove(Collection<String> filesOrDirs) throws IOException {
    for (String fileOrDir : filesOrDirs) {
      LOG.debug("Removing file {}", fileOrDir);
      removeOne(fileOrDir);
    }
  }

  private void removeOne(String fileOrDir) throws IOException {
    // Delete the file if it exists.
    boolean exists = Files.deleteIfExists(Paths.get(fileOrDir));
    if (!exists) {
      LOG.debug("Tried to delete {}, but it did not exist", fileOrDir);
    }
  }
}
