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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteSource;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharSource;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;

/**
 * Functions for zipping a directory (including a subdirectory) into a ZIP-file or unzipping it
 * again.
 */
@Internal
public final class ZipFiles {
  private ZipFiles() {}

  /**
   * Returns a new {@link ByteSource} for reading the contents of the given entry in the given zip
   * file.
   */
  static ByteSource asByteSource(ZipFile file, ZipEntry entry) {
    return new ZipEntryByteSource(file, entry);
  }

  /**
   * Returns a new {@link CharSource} for reading the contents of the given entry in the given zip
   * file as text using the given charset.
   */
  static CharSource asCharSource(ZipFile file, ZipEntry entry, Charset charset) {
    return asByteSource(file, entry).asCharSource(charset);
  }

  private static final class ZipEntryByteSource extends ByteSource {

    private final ZipFile file;
    private final ZipEntry entry;

    ZipEntryByteSource(ZipFile file, ZipEntry entry) {
      this.file = checkNotNull(file);
      this.entry = checkNotNull(entry);
    }

    @Override
    public InputStream openStream() throws IOException {
      return file.getInputStream(entry);
    }

    // TODO: implement size() to try calling entry.getSize()?

    @Override
    public String toString() {
      return "ZipFiles.asByteSource(" + file + ", " + entry + ")";
    }
  }

  /** Returns a {@link FluentIterable} of all the entries in the given zip file. */
  // unmodifiable Iterator<? extends ZipEntry> can be safely cast
  // to Iterator<ZipEntry>
  @SuppressWarnings("unchecked")
  static FluentIterable<ZipEntry> entries(final ZipFile file) {
    checkNotNull(file);
    return new FluentIterable<ZipEntry>() {
      @Override
      public Iterator<ZipEntry> iterator() {
        return (Iterator<ZipEntry>) Iterators.forEnumeration(file.entries());
      }
    };
  }

  /**
   * Unzips the zip file specified by the path and creates the directory structure <i>inside</i> the
   * target directory. Refuses to unzip files that refer to a parent directory, for security
   * reasons.
   *
   * @param zipFile the source zip-file to unzip
   * @param targetDirectory the directory to unzip to. If the zip-file contains any subdirectories,
   *     they will be created within our target directory.
   * @throws IOException the unzipping failed, e.g. because the output was not writable, the {@code
   *     zipFile} was not readable, or contains an illegal entry (contains "..", pointing outside
   *     the target directory)
   * @throws IllegalArgumentException the target directory is not a valid directory (e.g. does not
   *     exist, or is a file instead of a directory)
   */
  static void unzipFile(File zipFile, File targetDirectory) throws IOException {
    checkNotNull(zipFile);
    checkNotNull(targetDirectory);
    checkArgument(
        targetDirectory.isDirectory(),
        "%s is not a valid directory",
        targetDirectory.getAbsolutePath());
    try (ZipFile zipFileObj = new ZipFile(zipFile)) {
      for (ZipEntry entry : entries(zipFileObj)) {
        checkName(entry.getName());
        File targetFile = new File(targetDirectory, entry.getName());
        if (entry.isDirectory()) {
          if (!targetFile.isDirectory() && !targetFile.mkdirs()) {
            throw new IOException("Failed to create directory: " + targetFile.getAbsolutePath());
          }
        } else {
          File parentFile = targetFile.getParentFile();
          if (!parentFile.isDirectory() && !parentFile.mkdirs()) {
            throw new IOException("Failed to create directory: " + parentFile.getAbsolutePath());
          }
          // Write the file to the destination.
          asByteSource(zipFileObj, entry).copyTo(Files.asByteSink(targetFile));
        }
      }
    }
  }

  /**
   * Checks that the given entry name is legal for unzipping: if it contains ".." as a name element,
   * it could cause the entry to be unzipped outside the directory we're unzipping to.
   *
   * @throws IOException if the name is illegal
   */
  private static void checkName(String name) throws IOException {
    // First just check whether the entry name string contains "..".
    // This should weed out the the vast majority of entries, which will not
    // contain "..".
    if (name.contains("..")) {
      // If the string does contain "..", break it down into its actual name
      // elements to ensure it actually contains ".." as a name, not just a
      // name like "foo..bar" or even "foo..", which should be fine.
      File file = new File(name);
      while (file != null) {
        if ("..".equals(file.getName())) {
          throw new IOException(
              "Cannot unzip file containing an entry with " + "\"..\" in the name: " + name);
        }
        file = file.getParentFile();
      }
    }
  }

  /**
   * Zips an entire directory specified by the path.
   *
   * @param sourceDirectory the directory to read from. This directory and all subdirectories will
   *     be added to the zip-file. The path within the zip file is relative to the directory given
   *     as parameter, not absolute.
   * @param zipFile the zip-file to write to.
   * @throws IOException the zipping failed, e.g. because the input was not readable.
   */
  public static void zipDirectory(File sourceDirectory, File zipFile) throws IOException {
    checkNotNull(sourceDirectory);
    checkNotNull(zipFile);
    checkArgument(
        sourceDirectory.isDirectory(),
        "%s is not a valid directory",
        sourceDirectory.getAbsolutePath());
    checkArgument(
        !zipFile.exists(),
        "%s does already exist, files are not being overwritten",
        zipFile.getAbsolutePath());
    Closer closer = Closer.create();
    try {
      OutputStream outputStream =
          closer.register(new BufferedOutputStream(new FileOutputStream(zipFile)));
      zipDirectory(sourceDirectory, outputStream);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Zips an entire directory specified by the path.
   *
   * @param sourceDirectory the directory to read from. This directory and all subdirectories will
   *     be added to the zip-file. The path within the zip file is relative to the directory given
   *     as parameter, not absolute.
   * @param outputStream the stream to write the zip-file to. This method does not close
   *     outputStream.
   * @throws IOException the zipping failed, e.g. because the input was not readable.
   */
  public static void zipDirectory(File sourceDirectory, OutputStream outputStream)
      throws IOException {
    checkNotNull(sourceDirectory);
    checkNotNull(outputStream);
    checkArgument(
        sourceDirectory.isDirectory(),
        "%s is not a valid directory",
        sourceDirectory.getAbsolutePath());

    ZipOutputStream zos = new ZipOutputStream(outputStream);
    for (File file : sourceDirectory.listFiles()) {
      zipDirectoryInternal(file, "", zos);
    }
    zos.finish();
  }

  /**
   * Private helper function for zipping files. This one goes recursively through the input
   * directory and all of its subdirectories and adds the single zip entries.
   *
   * @param inputFile the file or directory to be added to the zip file
   * @param directoryName the string-representation of the parent directory name. Might be an empty
   *     name, or a name containing multiple directory names separated by "/". The directory name
   *     must be a valid name according to the file system limitations. The directory name should be
   *     empty or should end in "/".
   * @param zos the zipstream to write to
   * @throws IOException the zipping failed, e.g. because the output was not writeable.
   */
  private static void zipDirectoryInternal(
      File inputFile, String directoryName, ZipOutputStream zos) throws IOException {
    String entryName = directoryName + inputFile.getName();
    if (inputFile.isDirectory()) {
      entryName += "/";

      // We are hitting a sub-directory. Recursively add children to zip in deterministic,
      // sorted order.
      File[] childFiles = inputFile.listFiles();
      if (childFiles.length > 0) {
        Arrays.sort(childFiles);
        // loop through the directory content, and zip the files
        for (File file : childFiles) {
          zipDirectoryInternal(file, entryName, zos);
        }

        // Since this directory has children, exit now without creating a zipentry specific to
        // this directory. The entry for a non-entry directory is incompatible with certain
        // implementations of unzip.
        return;
      }
    }

    // Put the zip-entry for this file or empty directory into the zipoutputstream.
    ZipEntry entry = new ZipEntry(entryName);
    entry.setTime(inputFile.lastModified());
    zos.putNextEntry(entry);

    // Copy file contents into zipoutput stream.
    if (inputFile.isFile()) {
      Files.asByteSource(inputFile).copyTo(zos);
    }
  }
}
