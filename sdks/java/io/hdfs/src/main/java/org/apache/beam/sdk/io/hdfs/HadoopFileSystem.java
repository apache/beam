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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
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

/**
 * Adapts {@link org.apache.hadoop.fs.FileSystem} connectors to be used as
 * Apache Beam {@link FileSystem FileSystems}.
 *
 * <p>The following HDFS FileSystem(s) are known to be unsupported:
 * <ul>
 *   <li>FTPFileSystem: Missing seek support within FTPInputStream</li>
 * </ul>
 *
 * <p>This implementation assumes that the underlying Hadoop {@link FileSystem} is seek
 * efficient when reading. The source code for the following {@link FSInputStream} implementations
 * (as of Hadoop 2.7.1) do provide seek implementations:
 * <ul>
 *   <li>HarFsInputStream</li>
 *   <li>S3InputStream</li>
 *   <li>DFSInputStream</li>
 *   <li>SwiftNativeInputStream</li>
 *   <li>NativeS3FsInputStream</li>
 *   <li>LocalFSFileInputStream</li>
 *   <li>NativeAzureFsInputStream</li>
 *   <li>S3AInputStream</li>
 * </ul>
 */
class HadoopFileSystem extends FileSystem<HadoopResourceId> {
  @VisibleForTesting
  final org.apache.hadoop.fs.FileSystem fileSystem;

  HadoopFileSystem(Configuration configuration) throws IOException {
    this.fileSystem = org.apache.hadoop.fs.FileSystem.newInstance(configuration);
  }

  @Override
  protected List<MatchResult> match(List<String> specs) {
    ImmutableList.Builder<MatchResult> resultsBuilder = ImmutableList.builder();
    for (String spec : specs) {
      try {
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(spec));
        List<Metadata> metadata = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
          if (fileStatus.isFile()) {
            metadata.add(Metadata.builder()
                .setResourceId(new HadoopResourceId(fileStatus.getPath().toUri()))
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
  protected void copy(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    for (int i = 0; i < srcResourceIds.size(); ++i) {
      // Unfortunately HDFS FileSystems don't support a native copy operation so we are forced
      // to use the inefficient implementation found in FileUtil which copies all the bytes through
      // the local machine.
      //
      // HDFS FileSystem does define a concat method but could only find the DFSFileSystem
      // implementing it. The DFSFileSystem implemented concat by deleting the srcs after which
      // is not what we want. Also, all the other FileSystem implementations I saw threw
      // UnsupportedOperationException within concat.
      FileUtil.copy(
          fileSystem, srcResourceIds.get(i).toPath(),
          fileSystem, destResourceIds.get(i).toPath(),
          false,
          true,
          fileSystem.getConf());
    }
  }

  @Override
  protected void rename(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    for (int i = 0; i < srcResourceIds.size(); ++i) {
      fileSystem.rename(
          srcResourceIds.get(i).toPath(),
          destResourceIds.get(i).toPath());
    }
  }

  @Override
  protected void delete(Collection<HadoopResourceId> resourceIds) throws IOException {
    for (HadoopResourceId resourceId : resourceIds) {
      fileSystem.delete(resourceId.toPath(), false);
    }
  }

  @Override
  protected HadoopResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    try {
      if (singleResourceSpec.endsWith("/") && !isDirectory) {
        throw new IllegalArgumentException(String.format(
            "Expected file path but received directory path %s", singleResourceSpec));
      }
      return !singleResourceSpec.endsWith("/") && isDirectory
          ? new HadoopResourceId(new URI(singleResourceSpec + "/"))
          : new HadoopResourceId(new URI(singleResourceSpec));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid spec %s directory %s", singleResourceSpec, isDirectory),
          e);
    }
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
      return inputStream.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
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
    public SeekableByteChannel truncate(long size) throws IOException {
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
}
