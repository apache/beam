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
package org.apache.beam.runners.mapreduce.translation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;

/**
 * Translates a {@link Flatten} to a {@link FlattenOperation}.
 */
public class FlattenTranslator<T> extends TransformTranslator.Default<Flatten.PCollections<T>> {
  @Override
  public void translateNode(Flatten.PCollections<T> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    List<Graphs.Tag> inputTags = userGraphContext.getInputTags();
    Operation<?> operation;
    if (inputTags.isEmpty()) {
      // Create a empty source
      operation = new SourceReadOperation(new EmptySource(), userGraphContext.getOnlyOutputTag());
    } else {
      operation = new FlattenOperation();
    }
    context.addInitStep(
        Graphs.Step.of(userGraphContext.getStepName(), operation),
        inputTags,
        userGraphContext.getOutputTags());
  }

  private static class EmptySource extends BoundedSource<Void> {
    @Override
    public List<? extends BoundedSource<Void>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Collections.EMPTY_LIST;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<Void> createReader(PipelineOptions options) throws IOException {
      return new BoundedReader<Void>() {
        @Override
        public BoundedSource<Void> getCurrentSource() {
          return EmptySource.this;
        }

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
        public void close() throws IOException {
        }
      };
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }
}
