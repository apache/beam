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
package org.apache.beam.runners.twister2.translation.wrappers;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.twister2.Twister2TranslationContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;

/** Twister2 wrapper for Bounded Source. */
public class Twister2BoundedSource<T> extends BaseSourceFunc<WindowedValue<T>> {
  private static final Logger LOG = Logger.getLogger(Twister2BoundedSource.class.getName());

  private transient BoundedSource<T> source;
  private int numPartitions;
  private long splitSize = 100;
  private transient Config twister2Config;
  private List<? extends Source<T>> partitionedSources;
  private Source<T> localPartition;
  private transient PipelineOptions options;
  private String serializedOptions;
  private transient Iterator<WindowedValue<T>> readerIterator;
  private static final long DEFAULT_BUNDLE_SIZE = 64L * 1024L * 1024L;

  private boolean isInitialized = false;
  private byte[] sourceBytes;

  private Twister2BoundedSource() {
    isInitialized = false;
  }

  public Twister2BoundedSource(
      BoundedSource<T> boundedSource, Twister2TranslationContext context, PipelineOptions options) {
    source = boundedSource;
    this.options = options;
    this.serializedOptions = new SerializablePipelineOptions(options).toString();
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(options.as(PortablePipelineOptions.class)));
    RunnerApi.FunctionSpec sourceProto = ReadTranslation.toProto(source, components);
    sourceBytes = sourceProto.getPayload().toByteArray();
  }

  @Override
  public void prepare(TSetContext context) {
    initTransient();
    numPartitions = context.getParallelism();

    try {
      splitSize = source.getEstimatedSizeBytes(options) / numPartitions;
    } catch (Exception e) {
      LOG.warning(
          String.format(
              "Failed to get estimated bundle size for source %s, using default bundle "
                  + "size of %d bytes.",
              source.toString(), DEFAULT_BUNDLE_SIZE));
    }
    twister2Config = context.getConfig();
    int index = context.getIndex();
    List<Source<T>> partitionList = new ArrayList<>();
    try {
      partitionedSources = source.split(splitSize, options);
      // todo make sure the partitions are balanced.
      if (partitionedSources.size() == 0) {
        partitionList.add(source);
      } else if (numPartitions == partitionedSources.size()) {
        localPartition = partitionedSources.get(index);
        partitionList.add(localPartition);
      } else {
        // If we have more partitions than parallelism we need to allocate
        // more than one partition to each process and needs to be balanced
        int q = (int) Math.floor(((double) partitionedSources.size()) / numPartitions);
        int r = partitionedSources.size() % numPartitions;
        int assinedCount = q + ((index < r) ? 1 : 0);
        int startIndex = q * index + ((index < r) ? index : r);
        for (int i = startIndex; i < startIndex + assinedCount; i++) {
          partitionList.add(partitionedSources.get(i));
        }
      }

      final List<Source.Reader<T>> readers = new ArrayList<>();
      for (Source<T> tSource : partitionList) {
        BoundedSource.BoundedReader<T> reader = createReader(tSource);
        readers.add(reader);
      }
      readerIterator = new ReaderToIteratorAdapter<T>(readers);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create partitions for source " + source.getClass().getSimpleName(), e);
    }
  }

  @Override
  public boolean hasNext() {
    return readerIterator.hasNext();
  }

  @Override
  public WindowedValue<T> next() {
    return readerIterator.next();
  }

  private BoundedSource.BoundedReader<T> createReader(Source<T> partition) {
    try {
      return ((BoundedSource<T>) partition).createReader(options);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
    }
  }

  /**
   * Exposes an <code>Iterator</code>&lt;{@link WindowedValue}&gt; interface on top of a {@link
   * Source.Reader}.
   *
   * <p><code>hasNext</code> is idempotent and returns <code>true</code> iff further items are
   * available for reading using the underlying reader. Consequently, when the reader is closed, or
   * when the reader has no further elements available (i.e, {@link Source.Reader#advance()}
   * returned <code>false</code>), <code>hasNext</code> returns <code>false</code>.
   *
   * <p>Since this is a read-only iterator, an attempt to call <code>remove</code> will throw an
   * <code>UnsupportedOperationException</code>.
   */
  static class ReaderToIteratorAdapter<T> implements Iterator<WindowedValue<T>> {

    private static final boolean FAILED_TO_OBTAIN_NEXT = false;
    private static final boolean SUCCESSFULLY_OBTAINED_NEXT = true;

    private final List<Source.Reader<T>> readers;
    // current reader from the list
    Source.Reader<T> reader;
    private int readerIndex = 0;

    private boolean started = false;
    private boolean closed = false;
    private WindowedValue<T> next = null;

    public ReaderToIteratorAdapter(List<Source.Reader<T>> readers) {
      this.readers = readers;
      this.readerIndex = 0;
      this.reader = readers.get(readerIndex);
    }

    private boolean tryProduceNext() {
      try {
        if (seekNext()) {
          next =
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp());
          return SUCCESSFULLY_OBTAINED_NEXT;
        } else {
          return FAILED_TO_OBTAIN_NEXT;
        }

      } catch (final Exception e) {
        throw new RuntimeException("Failed to read data.", e);
      }
    }

    private void close() {
      try {
        reader.close();
        readerIndex++;
        if (readerIndex == readers.size()) {
          closed = true;
        } else {
          reader = readers.get(readerIndex);
          started = false;
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    private boolean seekNext() throws IOException {
      if (!started) {
        started = true;
        return reader.start() || advance(true);
      } else {
        return !closed && advance(false);
      }
    }

    private boolean advance(boolean calledAfterStart) throws IOException {
      if (!calledAfterStart && reader.advance()) {
        return true;
      } else {
        close();
        return seekNext();
      }
    }

    private WindowedValue<T> consumeCurrent() {
      if (next == null) {
        throw new NoSuchElementException();
      } else {
        final WindowedValue<T> current = next;
        next = null;
        return current;
      }
    }

    private WindowedValue<T> consumeNext() {
      if (next == null) {
        tryProduceNext();
      }
      return consumeCurrent();
    }

    @Override
    public boolean hasNext() {
      return next != null || tryProduceNext();
    }

    @Override
    public WindowedValue<T> next() {
      return consumeNext();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }
    options = new SerializablePipelineOptions(serializedOptions).get();
    source = (BoundedSource<T>) SerializableUtils.deserializeFromByteArray(sourceBytes, "WindowFn");
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
