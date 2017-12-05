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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.regions.Regions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration test, that reads from the real Kinesis.
 * You need to provide all {@link KinesisTestOptions} in order to run this.
 */
public class KinesisReaderIT {

  private static final long PIPELINE_STARTUP_TIME = TimeUnit.SECONDS.toMillis(10);
  private ExecutorService singleThreadExecutor = newSingleThreadExecutor();

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Ignore
  @Test
  public void readsDataFromRealKinesisStream()
      throws IOException, InterruptedException, ExecutionException {
    KinesisTestOptions options = readKinesisOptions();
    List<String> testData = prepareTestData(1000);

    Future<?> future = startTestPipeline(testData, options);
    KinesisUploader.uploadAll(testData, options);
    future.get();
  }

  private List<String> prepareTestData(int count) {
    List<String> data = newArrayList();
    for (int i = 0; i < count; ++i) {
      data.add(RandomStringUtils.randomAlphabetic(32));
    }
    return data;
  }

  private Future<?> startTestPipeline(List<String> testData, KinesisTestOptions options)
      throws InterruptedException {

    PCollection<String> result = p.
        apply(KinesisIO.read()
            .withStreamName(options.getAwsKinesisStream())
            .withInitialTimestampInStream(Instant.now())
            .withAWSClientsProvider(options.getAwsAccessKey(), options.getAwsSecretKey(),
                Regions.fromName(options.getAwsKinesisRegion()))
            .withMaxReadTime(Duration.standardMinutes(3))
        ).
        apply(ParDo.of(new RecordDataToString()));
    PAssert.that(result).containsInAnyOrder(testData);

    Future<?> future = singleThreadExecutor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        PipelineResult result = p.run();
        PipelineResult.State state = result.getState();
        while (state != PipelineResult.State.DONE && state != PipelineResult.State.FAILED) {
          Thread.sleep(1000);
          state = result.getState();
        }
        assertThat(state).isEqualTo(PipelineResult.State.DONE);
        return null;
      }
    });
    Thread.sleep(PIPELINE_STARTUP_TIME);
    return future;
  }

  private KinesisTestOptions readKinesisOptions() {
    PipelineOptionsFactory.register(KinesisTestOptions.class);
    return TestPipeline.testingPipelineOptions().as(KinesisTestOptions.class);
  }

  private static class RecordDataToString extends DoFn<KinesisRecord, String> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      checkNotNull(c.element(), "Null record given");
      c.output(new String(c.element().getData().array(), StandardCharsets.UTF_8));
    }
  }
}
