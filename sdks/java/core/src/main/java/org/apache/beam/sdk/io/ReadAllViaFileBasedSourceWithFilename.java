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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads each file of the input {@link PCollection} and outputs each element as the value of a
 * {@link KV}, where the key is the filename from which that value came.
 *
 * <p>Reads each {@link FileIO.ReadableFile} using given parameters for splitting files into offset
 * ranges and for creating a {@link FileBasedSource} for a file. The input {@link PCollection} must
 * not contain {@link ResourceId#isDirectory directories}.
 *
 * <p>To obtain the collection of {@link FileIO.ReadableFile} from a filepattern, use {@link
 * FileIO#readMatches()}.
 */
public class ReadAllViaFileBasedSourceWithFilename<T>
    extends ReadAllViaFileBasedSourceTransform<T, KV<String, T>> {

  public ReadAllViaFileBasedSourceWithFilename(
      final long desiredBundleSizeBytes,
      final SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      final Coder<KV<String, T>> coder) {
    super(desiredBundleSizeBytes, createSource, coder);
  }

  @Override
  protected DoFn<KV<FileIO.ReadableFile, OffsetRange>, KV<String, T>> readRangesFn() {
    return new ReadFileRangesFn<>(createSource, exceptionHandler);
  }

  private static class ReadFileRangesFn<T> extends AbstractReadFileRangesFn<T, KV<String, T>> {
    public ReadFileRangesFn(
        final SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
        final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler) {
      super(createSource, exceptionHandler);
    }

    @Override
    protected KV<String, T> makeOutput(
        final FileIO.ReadableFile file,
        final OffsetRange range,
        final FileBasedSource<T> fileBasedSource,
        final BoundedSource.BoundedReader<T> reader) {
      return KV.of(file.getMetadata().resourceId().toString(), reader.getCurrent());
    }
  }
}
