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

import static org.junit.Assert.assertEquals;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * Implements a FileSystem with verification tests.
 *
 * <p>Not all operations are fully supported or are well behaved. File and directory contents are
 * not stored.
 *
 * <p>This class was created to verify that IO parameters (such as withKmsKey in FileIO) get passed
 * down to FileSystems calls. Since FileSystems is implemented as static methods that makes it hard
 * to intercept these calls.
 *
 * <p>Usage: Create a FileSystemRegistrar class that instantiates a FakeFileSystem with a unique
 * scheme, then do I/O on files using that scheme. Search the codebase for examples.
 */
@AutoValue
abstract class FakeFileSystem extends FileSystem<FakeResourceId> {

  @Override
  protected abstract String getScheme();

  @Nullable
  public abstract String getRequireKmsKey();

  static Builder builder() {
    return new AutoValue_FakeFileSystem.Builder().setRequireKmsKey(null);
  }

  @AutoValue.Builder
  abstract static class Builder {
    /** Required URI prefix for this FS. Should be unique among all tests. */
    abstract Builder setScheme(String scheme);

    /**
     * Expect this KMS key in operations that use CreateOptions and MoveOptions. A value of null
     * means no verification is performed.
     */
    abstract Builder setRequireKmsKey(@Nullable String kmsKey);

    abstract FakeFileSystem build();
  }

  /** A channel that has no output and discards all input. */
  private static class NullByteChannel implements ReadableByteChannel, WritableByteChannel {

    private boolean open = true;

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
      return 0;
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
      int written = byteBuffer.remaining();
      byteBuffer.position(byteBuffer.position() + written);
      return written;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() throws IOException {
      open = false;
    }
  }

  /** Always finds files. */
  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<Metadata> metadata = new ArrayList<>();
    for (String spec : specs) {
      metadata.add(
          Metadata.builder()
              .setResourceId(new FakeResourceId(getScheme(), parseSpec(spec), false))
              .setIsReadSeekEfficient(false)
              .setSizeBytes(0)
              .build());
    }
    List<MatchResult> result = new ArrayList<>();
    result.add(MatchResult.create(Status.OK, metadata));
    return result;
  }

  @Override
  protected ReadableByteChannel open(FakeResourceId resourceId) throws IOException {
    return new NullByteChannel();
  }

  private String parseSpec(String resourceSpec) {
    String prefix = getScheme() + "://";
    if (!resourceSpec.startsWith(prefix)) {
      throw new UnsupportedOperationException(
          String.format("expected prefix of %s: %s", prefix, resourceSpec));
    }
    return resourceSpec.substring(prefix.length());
  }

  @Override
  protected FakeResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    return new FakeResourceId(getScheme(), parseSpec(singleResourceSpec), isDirectory);
  }

  @Override
  protected WritableByteChannel create(FakeResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    if (getRequireKmsKey() != null) {
      assertEquals(getRequireKmsKey(), createOptions.kmsKey());
    }
    return new NullByteChannel();
  }

  @Override
  protected void copy(
      List<FakeResourceId> srcResourceIds,
      List<FakeResourceId> destResourceIds,
      MoveOptions moveOptions)
      throws IOException {
    if (getRequireKmsKey() != null) {
      assertEquals(getRequireKmsKey(), moveOptions.getDestKmsKey());
    }
  }

  @Override
  protected void rename(
      List<FakeResourceId> srcResourceIds,
      List<FakeResourceId> destResourceIds,
      MoveOptions moveOptions)
      throws IOException {
    if (getRequireKmsKey() != null) {
      assertEquals(getRequireKmsKey(), moveOptions.getDestKmsKey());
    }
  }

  @Override
  protected void delete(Collection<FakeResourceId> resourceIds, MoveOptions moveOptions)
      throws IOException {
    // Don't check kmsKey on deletes, since it doesn't matter for this operation.
  }

  /**
   * This scheme performs stateless checks of the presence of kmsKey settings on relevant
   * operations.
   */
  @AutoService(FileSystemRegistrar.class)
  @Experimental(Kind.FILESYSTEM)
  public static class TestKmsKeyFileSystemRegistrar implements FileSystemRegistrar {
    @Override
    public Iterable<FileSystem> fromOptions(@Nullable PipelineOptions options) {
      return ImmutableList.of(
          FakeFileSystem.builder()
              .setScheme("testkmskey")
              .setRequireKmsKey("test_kms_key")
              .build());
    }
  }
}
