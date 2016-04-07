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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link View} for a {@link DataflowPipelineRunner}. */
@RunWith(JUnit4.class)
public class DataflowViewTest {
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private Pipeline createTestBatchRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("someproject");
    options.setStagingLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(null);
    return Pipeline.create(options);
  }

  private Pipeline createTestStreamingRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setStreaming(true);
    options.setProject("someproject");
    options.setStagingLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(null);
    return Pipeline.create(options);
  }

  private void testViewUnbounded(
      Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("non-bounded PCollection")));
    pipeline
        .apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> apply(PBegin input) {
                return PCollection.<KV<String, Integer>>createPrimitiveOutputInternal(
                        input.getPipeline(),
                        WindowingStrategy.globalDefault(),
                        PCollection.IsBounded.UNBOUNDED)
                    .setTypeDescriptorInternal(new TypeDescriptor<KV<String, Integer>>() {});
              }
            })
        .apply(view);
  }

  private void testViewNonmerging(
      Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("Consumed by GroupByKey")));
    pipeline.apply(Create.<KV<String, Integer>>of(KV.of("hello", 5)))
        .apply(Window.<KV<String, Integer>>into(new InvalidWindows<>(
            "Consumed by GroupByKey", FixedWindows.of(Duration.standardHours(1)))))
        .apply(view);
  }

  @Test
  public void testViewUnboundedAsSingletonBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsSingletonStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsIterableBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsIterableStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsListBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsListStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsMapBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewUnboundedAsMapStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewUnboundedAsMultimapBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewUnboundedAsMultimapStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsSingletonBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsSingletonStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsIterableBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsIterableStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsListBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsListStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsMapBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewNonmergingAsMapStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewNonmergingAsMultimapBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsMultimapStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<String, Integer>asMultimap());
  }
}

