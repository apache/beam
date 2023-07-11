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

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads each file in the input {@link PCollection} of {@link ReadableFile} using given parameters
 * for splitting files into offset ranges and for creating a {@link FileBasedSource} for a file. The
 * input {@link PCollection} must not contain {@link ResourceId#isDirectory directories}.
 *
 * <p>To obtain the collection of {@link ReadableFile} from a filepattern, use {@link
 * FileIO#readMatches()}.
 */
public class ReadAllViaFileBasedSource<T> extends ReadAllViaFileBasedSourceTransform<T, T> {
  public ReadAllViaFileBasedSource(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      Coder<T> coder) {
    super(
        desiredBundleSizeBytes,
        createSource,
        coder,
        DEFAULT_USES_RESHUFFLE,
        new ReadFileRangesFnExceptionHandler());
  }

  public ReadAllViaFileBasedSource(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      Coder<T> coder,
      boolean usesReshuffle,
      ReadFileRangesFnExceptionHandler exceptionHandler) {
    super(desiredBundleSizeBytes, createSource, coder, usesReshuffle, exceptionHandler);
  }

  @Override
  protected DoFn<KV<ReadableFile, OffsetRange>, T> readRangesFn() {
    return new ReadFileRangesFn<>(createSource, exceptionHandler);
  }

  private static class ReadFileRangesFn<T> extends AbstractReadFileRangesFn<T, T> {
    public ReadFileRangesFn(
        final SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
        final ReadFileRangesFnExceptionHandler exceptionHandler) {
      super(createSource, exceptionHandler);
    }

    @Override
    protected T makeOutput(
        final ReadableFile file,
        final OffsetRange range,
        final FileBasedSource<T> fileBasedSource,
        final BoundedSource.BoundedReader<T> reader) {
      return reader.getCurrent();
    }
  }

  /** A class to handle errors which occur during file reads. */
  public static class ReadFileRangesFnExceptionHandler implements Serializable {

    /*
     * Applies the desired handler logic to the given exception and returns
     * if the exception should be thrown.
     */
    public boolean apply(ReadableFile file, OffsetRange range, Exception e) {
      return true;
    }
  }
}
