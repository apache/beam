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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.sdk.values.WindowedValues.valueInGlobalWindow;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SparkDatasetTranslationContext}. */
@RunWith(JUnit4.class)
public class SparkDatasetTranslationContextTest {

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();

  private static final String COLLECTION = "collection";

  /** Names of the Datasets that were evaluated, recorded from the executor side. */
  private static final Set<String> EVALUATED = ConcurrentHashMap.newKeySet();

  private SparkDatasetTranslationContext context;

  @After
  public void tearDown() {
    if (context != null) {
      context.getSparkSession().sharedState().cacheManager().clearCache();
    }
  }

  @Test
  public void datasetReadBySeveralTransformsIsPersisted() {
    SparkPipelineOptions options = options();
    context = newContext(options);
    context.addConsumer(COLLECTION);
    context.addConsumer(COLLECTION);

    context.putDataset(COLLECTION, dataset());

    assertEquals(
        StorageLevel.fromString(options.getStorageLevel()),
        context.getDataset(COLLECTION).storageLevel());
  }

  @Test
  public void datasetReadByOneTransformIsNotPersisted() {
    context = newContext(options());
    context.addConsumer(COLLECTION);

    context.putDataset(COLLECTION, dataset());

    assertEquals(StorageLevel.NONE(), context.getDataset(COLLECTION).storageLevel());
  }

  @Test
  public void projectionOfPersistedRowsIsNotPersistedAgain() {
    context = newContext(options());
    context.addConsumer(COLLECTION);
    context.addConsumer(COLLECTION);

    context.putDataset(COLLECTION, dataset(), false);

    assertEquals(StorageLevel.NONE(), context.getDataset(COLLECTION).storageLevel());
  }

  @Test
  public void cacheDisabledSkipsPersisting() {
    SparkPipelineOptions options = options();
    options.setCacheDisabled(true);
    context = newContext(options);
    context.addConsumer(COLLECTION);
    context.addConsumer(COLLECTION);

    context.putDataset(COLLECTION, dataset());

    assertEquals(StorageLevel.NONE(), context.getDataset(COLLECTION).storageLevel());
  }

  @Test
  public void computeOutputsEvaluatesOnlyUnconsumedDatasets() {
    EVALUATED.clear();
    context = newContext(options());
    context.putDataset("leaf", recording("leaf"));
    context.putDataset("consumed", recording("consumed"));
    context.getDataset("consumed");

    context.computeOutputs();

    assertEquals(Collections.singleton("leaf"), EVALUATED);
  }

  private static SparkPipelineOptions options() {
    return PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
  }

  private static SparkDatasetTranslationContext newContext(SparkPipelineOptions options) {
    return new SparkDatasetTranslationContext(
        contextRule.getSparkContext(),
        options,
        JobInfo.create("job", "job", "token", PipelineOptionsTranslation.toProto(options)));
  }

  private static Encoder<WindowedValue<byte[]>> encoder() {
    return EncoderHelpers.windowedValueEncoder(
        EncoderHelpers.encoderFor(ByteArrayCoder.of()),
        EncoderHelpers.encoderFor(GlobalWindow.Coder.INSTANCE));
  }

  /** A one element Dataset with a unique payload, so no two tests share a cached plan. */
  private Dataset<WindowedValue<byte[]>> dataset() {
    byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    return context
        .getSparkSession()
        .createDataset(Collections.singletonList(valueInGlobalWindow(payload)), encoder());
  }

  /** A Dataset that records {@code name} in {@link #EVALUATED} when its rows are computed. */
  private Dataset<WindowedValue<byte[]>> recording(String name) {
    return dataset()
        .map(
            (MapFunction<WindowedValue<byte[]>, WindowedValue<byte[]>>)
                value -> {
                  EVALUATED.add(name);
                  return value;
                },
            encoder());
  }
}
