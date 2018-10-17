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
package org.apache.beam.sdk.io.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapts {@link org.apache.hadoop.fs.FileSystem} connectors to be used as Apache Beam {@link
 * FileSystem FileSystems}.
 *
 * <p>The following HDFS FileSystem(s) are known to be unsupported:
 *
 * <ul>
 *   <li>FTPFileSystem: Missing seek support within FTPInputStream
 * </ul>
 *
 * <p>This implementation assumes that the underlying Hadoop {@link FileSystem} is seek efficient
 * when reading. The source code for the following {@link FSInputStream} implementations (as of
 * Hadoop 2.7.1) do provide seek implementations:
 *
 * <ul>
 *   <li>HarFsInputStream
 *   <li>S3InputStream
 *   <li>DFSInputStream
 *   <li>SwiftNativeInputStream
 *   <li>NativeS3FsInputStream
 *   <li>LocalFSFileInputStream
 *   <li>NativeAzureFsInputStream
 *   <li>S3AInputStream
 * </ul>
 */
class HadoopFileSystem extends FileSystem<HadoopResourceId> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystem.class);

  @VisibleForTesting static final String LOG_CREATE_DIRECTORY = "Creating directory %s";
  @VisibleForTesting final org.apache.hadoop.fs.FileSystem fileSystem;

  HadoopFileSystem(Configuration configuration) throws IOException {
    this.fileSystem = org.apache.hadoop.fs.FileSystem.newInstance(configuration);
  }

  @Override
  protected List<MatchResult> match(List<String> specs) {
    ImmutableList.Builder<MatchResult> resultsBuilder = ImmutableList.builder();
    for (String spec : specs) {
      try {
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(spec));
        if (fileStatuses == null) {
          resultsBuilder.add(MatchResult.create(Status.NOT_FOUND, Collections.emptyList()));
          continue;
        }

        List<Metadata> metadata = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
          if (fileStatus.isFile()) {
            URI uri = dropEmptyAuthority(fileStatus.getPath().toUri().toString());
            metadata.add(
                Metadata.builder()
                    .setResourceId(new HadoopResourceId(uri))
                    .setIsReadSeekEfficient(true)
                    .setSizeBytes(fileStatus.getLen())
                    .build());
          }
        }
        resultsBuilder.add(MatchResult.create(Status.OK, metadata));
      } catch (IOException e) {
        resultsBuilder.add(MatchResult.create(Status.ERROR, e));
      }
    }
    return resultsBuilder.build();
  }

  @Override
  protected WritableByteChannel create(HadoopResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return Channels.newChannel(fileSystem.create(resourceId.toPath()));
  }

  @Override
  protected ReadableByteChannel open(HadoopResourceId resourceId) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(resourceId.toPath());
    return new HadoopSeekableByteChannel(fileStatus, fileSystem.open(resourceId.toPath()));
  }

  @Override
  protected void copy(List<HadoopResourceId> srcResourceIds, List<HadoopResourceId> destResourceIds)
      throws IOException {
    for (int i = 0; i < srcResourceIds.size(); ++i) {
      // Unfortunately HDFS FileSystems don't support a native copy operation so we are forced
      // to use the inefficient implementation found in FileUtil which copies all the bytes through
      // the local machine.
      //
      // HDFS FileSystem does define a concat method but could only find the DFSFileSystem
      // implementing it. The DFSFileSystem implemented concat by deleting the srcs after which
      // is not what we want. Also, all the other FileSystem implementations I saw threw
      // UnsupportedOperationException within concat.
      boolean success =
          FileUtil.copy(
              fileSystem,
              srcResourceIds.get(i).toPath(),
              fileSystem,
              destResourceIds.get(i).toPath(),
              false,
              true,
              fileSystem.getConf());
      if (!success) {
        // Defensive coding as this should not happen in practice
        throw new IOException(
            String.format(
                "Unable to copy resource %s to %s. No further information provided by underlying filesystem.",
                srcResourceIds.get(i).toPath(), destResourceIds.get(i).toPath()));
      }
    }
  }

  /**
   * Renames a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources. Destination
   * resources will be created recursively.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   * @throws FileNotFoundException if the source resources are missing. When rename throws, the
   *     state of the resources is unknown but safe: for every (source, destination) pair of
   *     resources, the following are possible: a) source exists, b) destination exists, c) source
   *     and destination both exist. Thus no data is lost, however, duplicated resource are
   *     possible. In such scenarios, callers can use {@code match()} to determine the state of the
   *     resource.
   * @throws FileAlreadyExistsException if the target resources already exist.
   * @throws IOException if the underlying filesystem indicates the rename was not performed but no
   *     other errors were thrown.
   */
  @Override
  protected void rename(
      List<HadoopResourceId> srcResourceIds, List<HadoopResourceId> destResourceIds)
      throws IOException {
    for (int i = 0; i < srcResourceIds.size(); ++i) {

      // rename in HDFS requires the target directory to exist or silently fails (BEAM-4861)
      Path targetDirectory = destResourceIds.get(i).toPath().getParent();
      if (!fileSystem.exists(targetDirectory)) {
        LOG.debug(
            String.format(
                LOG_CREATE_DIRECTORY, Path.getPathWithoutSchemeAndAuthority(targetDirectory)));
        boolean success = fileSystem.mkdirs(targetDirectory);
        if (!success) {
          throw new IOException(
              String.format(
                  "Unable to create target directory %s. No further information provided by underlying filesystem.",
                  targetDirectory));
        }
      }

      boolean success =
          fileSystem.rename(srcResourceIds.get(i).toPath(), destResourceIds.get(i).toPath());
      if (!success) {
        if (!fileSystem.exists(srcResourceIds.get(i).toPath())) {
          throw new FileNotFoundException(
              String.format(
                  "Unable to rename resource %s to %s as source not found.",
                  srcResourceIds.get(i).toPath(), destResourceIds.get(i).toPath()));

        } else if (fileSystem.exists(destResourceIds.get(i).toPath())) {
          throw new FileAlreadyExistsException(
              String.format(
                  "Unable to rename resource %s to %s as destination already exists.",
                  srcResourceIds.get(i).toPath(), destResourceIds.get(i).toPath()));

        } else {
          throw new IOException(
              String.format(
                  "Unable to rename resource %s to %s. No further information provided by underlying filesystem.",
                  srcResourceIds.get(i).toPath(), destResourceIds.get(i).toPath()));
        }
      }
    }
  }

  @Override
  protected void delete(Collection<HadoopResourceId> resourceIds) throws IOException {
    for (HadoopResourceId resourceId : resourceIds) {
      // ignore response as issues are surfaced with exception
      fileSystem.delete(resourceId.toPath(), false);
    }
  }

  @Override
  protected HadoopResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (singleResourceSpec.endsWith("/") && !isDirectory) {
      throw new IllegalArgumentException(
          String.format("Expected file path but received directory path %s", singleResourceSpec));
    }
    return !singleResourceSpec.endsWith("/") && isDirectory
        ? new HadoopResourceId(dropEmptyAuthority(singleResourceSpec + "/"))
        : new HadoopResourceId(dropEmptyAuthority(singleResourceSpec));
  }

  @Override
  protected String getScheme() {
    return fileSystem.getScheme();
  }

  /** An adapter around {@link FSDataInputStream} that implements {@link SeekableByteChannel}. */
  private static class HadoopSeekableByteChannel implements SeekableByteChannel {
    private final FileStatus fileStatus;
    private final FSDataInputStream inputStream;
    private boolean closed;

    private HadoopSeekableByteChannel(FileStatus fileStatus, FSDataInputStream inputStream) {
      this.fileStatus = fileStatus;
      this.inputStream = inputStream;
      this.closed = false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      // O length read must be supported
      int read = 0;
      // We avoid using the ByteBuffer based read for Hadoop because some FSDataInputStream
      // implementations are not ByteBufferReadable,
      // See https://issues.apache.org/jira/browse/HADOOP-14603
      if (dst.hasArray()) {
        // does the same as inputStream.read(dst):
        // stores up to dst.remaining() bytes into dst.array() starting at dst.position().
        // But dst can have an offset with its backing array hence the + dst.arrayOffset()
        read = inputStream.read(dst.array(), dst.position() + dst.arrayOffset(), dst.remaining());
      } else {
        // TODO: Add support for off heap ByteBuffers in case the underlying FSDataInputStream
        // does not support reading from a ByteBuffer.
        read = inputStream.read(dst);
      }
      if (read > 0) {
        dst.position(dst.position() + read);
      }
      return read;
    }

    @Override
    public int write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      return inputStream.getPos();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      inputStream.seek(newPosition);
      return this;
    }

    @Override
    public long size() throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      return fileStatus.getLen();
    }

    @Override
    public SeekableByteChannel truncate(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
      return !closed;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      inputStream.close();
    }
  }

  private static URI dropEmptyAuthority(String uriStr) {
    URI uri = URI.create(uriStr);
    String prefix = uri.getScheme() + ":///";
    if (uriStr.startsWith(prefix)) {
      return URI.create(uri.getScheme() + ":/" + uriStr.substring(prefix.length()));
    } else {
      return uri;
    }
  }
}
