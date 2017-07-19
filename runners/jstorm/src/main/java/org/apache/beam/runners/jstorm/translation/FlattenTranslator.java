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
package org.apache.beam.runners.jstorm.translation;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Translates a {@link Flatten} to a JStorm {@link FlattenExecutor}.
 * @param <V>
 */
class FlattenTranslator<V> extends TransformTranslator.Default<Flatten.PCollections<V>> {

  @Override
  public void translateNode(Flatten.PCollections<V> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();

    // Since a new tag is created in PCollectionList, retrieve the real tag here.
    Map<TupleTag<?>, PValue> inputs = Maps.newHashMap();
    for (Map.Entry<TupleTag<?>, PValue> entry : userGraphContext.getInputs().entrySet()) {
      PCollection<V> pc = (PCollection<V>) entry.getValue();
      inputs.putAll(pc.expand());
    }
    String description = describeTransform(transform, inputs, userGraphContext.getOutputs());

    if (inputs.isEmpty()) {
      // Create a empty source
      TupleTag<?> tag = userGraphContext.getOutputTag();
      PValue output = userGraphContext.getOutput();

      UnboundedSourceSpout spout = new UnboundedSourceSpout(
          description,
          new EmptySource(),
          userGraphContext.getOptions(),
          tag);
      context.getExecutionGraphContext().registerSpout(spout, TaggedPValue.of(tag, output));

    } else {
      FlattenExecutor executor = new FlattenExecutor(description, userGraphContext.getOutputTag());
      context.addTransformExecutor(executor, inputs, userGraphContext.getOutputs());
    }
  }

  private static class EmptySource extends UnboundedSource<Void, UnboundedSource.CheckpointMark> {
    @Override
    public List<? extends UnboundedSource<Void, CheckpointMark>> split(
        int i, PipelineOptions pipelineOptions) throws Exception {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Void> createReader(
        PipelineOptions pipelineOptions,
        @Nullable CheckpointMark checkpointMark) throws IOException {
      return new EmptyReader();
    }

    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    private class EmptyReader extends UnboundedReader<Void> {
      @Override
      public boolean start() throws IOException {
        return false;
      }

      @Override
      public boolean advance() throws IOException {
        return false;
      }

      @Override
      public Void getCurrent() throws NoSuchElementException {
        throw new NoSuchElementException();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        throw new NoSuchElementException();
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public Instant getWatermark() {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return null;
      }

      @Override
      public UnboundedSource<Void, ?> getCurrentSource() {
        return EmptySource.this;
      }
    }
  }
}
