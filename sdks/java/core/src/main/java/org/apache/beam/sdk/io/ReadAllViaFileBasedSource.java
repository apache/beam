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

import com.google.auto.value.AutoValue;
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
public class ReadAllViaFileBasedSource<T, K> extends ReadAllViaFileBasedSourceTransform<T, K> {

  private final SerializableFunction<OutputContextFromFile<T>, K> outputFn;

  protected ReadAllViaFileBasedSource(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      Coder<K> coder,
      SerializableFunction<OutputContextFromFile<T>, K> outputFn) {
    super(
        desiredBundleSizeBytes,
        createSource,
        coder,
        DEFAULT_USES_RESHUFFLE,
        new ReadAllViaFileBasedSourceTransform.ReadFileRangesFnExceptionHandler());
    this.outputFn = outputFn;
  }

  protected ReadAllViaFileBasedSource(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      Coder<K> coder,
      boolean usesReshuffle,
      ReadAllViaFileBasedSourceTransform.ReadFileRangesFnExceptionHandler exceptionHandler,
      SerializableFunction<OutputContextFromFile<T>, K> outputFn) {
    super(desiredBundleSizeBytes, createSource, coder, usesReshuffle, exceptionHandler);
    this.outputFn = outputFn;
  }

  public static <InputT> ReadAllViaFileBasedSource<InputT, InputT> create(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<InputT>> createSource,
      Coder<InputT> coder,
      boolean usesReshuffle,
      ReadAllViaFileBasedSourceTransform.ReadFileRangesFnExceptionHandler exceptionHandler) {
    return new ReadAllViaFileBasedSource<>(
        desiredBundleSizeBytes,
        createSource,
        coder,
        usesReshuffle,
        exceptionHandler,
        outputArguments -> outputArguments.reader().getCurrent());
  }

  public static <InputT> ReadAllViaFileBasedSource<InputT, InputT> create(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<InputT>> createSource,
      Coder<InputT> coder) {
    return create(
        desiredBundleSizeBytes,
        createSource,
        coder,
        outputArguments -> outputArguments.reader().getCurrent());
  }

  public static <InputT, OutputT> ReadAllViaFileBasedSource<InputT, OutputT> create(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<InputT>> createSource,
      Coder<OutputT> coder,
      SerializableFunction<OutputContextFromFile<InputT>, OutputT> outputFn) {
    return new ReadAllViaFileBasedSource<>(desiredBundleSizeBytes, createSource, coder, outputFn);
  }

  @Override
  protected DoFn<KV<ReadableFile, OffsetRange>, K> readRangesFn() {
    return new ReadFileRangesFn<>(outputFn, createSource, exceptionHandler);
  }

  private static class ReadFileRangesFn<T, K> extends AbstractReadFileRangesFn<T, K> {
    private final SerializableFunction<OutputContextFromFile<T>, K> outputFn;

    public ReadFileRangesFn(
        final SerializableFunction<OutputContextFromFile<T>, K> outputFn,
        final SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
        final ReadAllViaFileBasedSourceTransform.ReadFileRangesFnExceptionHandler
            exceptionHandler) {
      super(createSource, exceptionHandler);
      this.outputFn = outputFn;
    }

    @Override
    protected K makeOutput(
        final ReadableFile file,
        final OffsetRange range,
        final FileBasedSource<T> fileBasedSource,
        final BoundedSource.BoundedReader<T> reader) {
      return outputFn.apply(OutputContextFromFile.create(file, range, fileBasedSource, reader));
    }
  }

  /** Data carrier for the arguments of the {@link ReadFileRangesFn#makeOutput} method. */
  @AutoValue
  public abstract static class OutputContextFromFile<ReadT> {
    public abstract FileIO.ReadableFile file();

    public abstract OffsetRange range();

    public abstract FileBasedSource<ReadT> fileBasedSource();

    public abstract BoundedSource.BoundedReader<ReadT> reader();

    public static <ReadT> OutputContextFromFile<ReadT> create(
        final ReadableFile file,
        final OffsetRange range,
        final FileBasedSource<ReadT> fileBasedSource,
        final BoundedSource.BoundedReader<ReadT> reader) {
      return new AutoValue_ReadAllViaFileBasedSource_OutputContextFromFile<>(
          file, range, fileBasedSource, reader);
    }
  }
}
