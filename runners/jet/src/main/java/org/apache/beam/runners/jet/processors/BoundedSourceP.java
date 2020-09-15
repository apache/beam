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
package org.apache.beam.runners.jet.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for reading from a bounded Beam
 * source.
 */
public class BoundedSourceP<T> extends AbstractProcessor implements Traverser {

  private final Traverser<BoundedSource<T>> shardsTraverser;
  private final PipelineOptions options;
  private final Coder outputCoder;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final String ownerId; // do not remove it, very useful for debugging

  private BoundedSource.BoundedReader currentReader;

  BoundedSourceP(
      List<BoundedSource<T>> shards, PipelineOptions options, Coder outputCoder, String ownerId) {
    this.shardsTraverser = Traversers.traverseIterable(shards);
    this.options = options;
    this.outputCoder = outputCoder;
    this.ownerId = ownerId;
  }

  @Override
  protected void init(@Nonnull Processor.Context context) throws Exception {
    nextShard();
  }

  @Override
  public Object next() {
    if (currentReader == null) {
      return null;
    }
    try {
      Object item = currentReader.getCurrent();
      WindowedValue<Object> res =
          WindowedValue.timestampedValueInGlobalWindow(item, currentReader.getCurrentTimestamp());
      if (!currentReader.advance()) {
        nextShard();
      }
      return outputCoder == null
          ? res
          : Utils.encode(
              res, outputCoder); // todo: this is not nice, have done this only as a quick fix for
      // BoundedSourcePTest
    } catch (IOException e) {
      throw ExceptionUtil.rethrow(e);
    }
  }

  /**
   * Called when currentReader is null or drained. At the end it will contain a started reader of
   * the next shard or null.
   */
  private void nextShard() throws IOException {
    for (; ; ) {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      BoundedSource<T> shard = shardsTraverser.next();
      if (shard == null) {
        break; // all shards done
      }
      currentReader = shard.createReader(options);
      if (currentReader.start()) {
        break;
      }
    }
  }

  @Override
  public boolean complete() {
    return emitFromTraverser(this);
  }

  @Override
  public boolean isCooperative() {
    return false;
  }

  @Override
  public void close() throws Exception {
    if (currentReader != null) {
      currentReader.close();
    }
  }

  public static <T> ProcessorMetaSupplier supplier(
      BoundedSource<T> boundedSource,
      SerializablePipelineOptions options,
      Coder outputCoder,
      String ownerId) {
    return new BoundedSourceMetaProcessorSupplier<>(boundedSource, options, outputCoder, ownerId);
  }

  private static class BoundedSourceMetaProcessorSupplier<T> implements ProcessorMetaSupplier {

    private final BoundedSource<T> boundedSource;
    private final SerializablePipelineOptions options;
    private final Coder outputCoder;
    private final String ownerId;

    private transient List<? extends BoundedSource<T>> shards;

    private BoundedSourceMetaProcessorSupplier(
        BoundedSource<T> boundedSource,
        SerializablePipelineOptions options,
        Coder outputCoder,
        String ownerId) {
      this.boundedSource = boundedSource;
      this.options = options;
      this.outputCoder = outputCoder;
      this.ownerId = ownerId;
    }

    @Override
    public void init(@Nonnull ProcessorMetaSupplier.Context context) throws Exception {
      long desiredSizeBytes =
          Math.max(
              1, boundedSource.getEstimatedSizeBytes(options.get()) / context.totalParallelism());
      shards = boundedSource.split(desiredSizeBytes, options.get());
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(
        @Nonnull List<Address> addresses) {
      return address ->
          new BoundedSourceProcessorSupplier(
              Utils.roundRobinSubList(shards, addresses.indexOf(address), addresses.size()),
              options,
              outputCoder,
              ownerId);
    }
  }

  private static class BoundedSourceProcessorSupplier<T> implements ProcessorSupplier {
    private final List<BoundedSource<T>> shards;
    private final SerializablePipelineOptions options;
    private final Coder outputCoder;
    private final String ownerId;
    private transient ProcessorSupplier.Context context;

    private BoundedSourceProcessorSupplier(
        List<BoundedSource<T>> shards,
        SerializablePipelineOptions options,
        Coder outputCoder,
        String ownerId) {
      this.shards = shards;
      this.options = options;
      this.outputCoder = outputCoder;
      this.ownerId = ownerId;
    }

    @Override
    public void init(@Nonnull Context context) {
      this.context = context;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
      int indexBase = context.memberIndex() * context.localParallelism();
      List<Processor> res = new ArrayList<>(count);
      for (int i = 0; i < count; i++, indexBase++) {
        res.add(
            new BoundedSourceP<>(
                Utils.roundRobinSubList(shards, i, count), options.get(), outputCoder, ownerId));
      }
      return res;
    }
  }
}
