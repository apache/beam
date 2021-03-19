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
package org.apache.beam.examples.cookbook;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryStorageAPIStreamingIT {
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Value {
    public abstract long getNumber();

    @Nullable
    public abstract ByteBuffer getPayload();
  }

  public interface Options extends TestPipelineOptions {
    @Description("The number of records per second to generate.")
    @Default.Integer(10000)
    Integer getRecordsPerSecond();

    void setRecordsPerSecond(Integer recordsPerSecond);

    @Description("The size of the records to write in bytes.")
    @Default.Integer(1024)
    Integer getPayloadSizeBytes();

    void setPayloadSizeBytes(Integer payloadSizeBytes);

    @Description("Parallelism used for Storage API writes.")
    @Default.Integer(5)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Description("Frequency to trigger appends. Each shard triggers independently.")
    @Default.Integer(5)
    Integer getTriggerFrequencySec();

    void setTriggerFrequencySec(Integer triggerFrequencySec);

    @Description("The table to write to.")
    String getTargetTable();

    void setTargetTable(String table);
  }

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(Options.class);
  }

  @Test
  public void testStorageAPIStreaming() throws Exception {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    Pipeline p = Pipeline.create(options);
    final int payloadSizeBytes = options.getPayloadSizeBytes();

    // Generate input.
    PCollection<Value> values =
        p.apply(
                GenerateSequence.from(1)
                    .to(1000000)
                    .withRate(options.getRecordsPerSecond(), Duration.standardSeconds(1)))
            .apply(
                MapElements.into(TypeDescriptor.of(Value.class))
                    .via(
                        l -> {
                          byte[] payload = "".getBytes(StandardCharsets.UTF_8);
                          if (payloadSizeBytes > 0) {
                            payload = new byte[payloadSizeBytes];
                            ThreadLocalRandom.current().nextBytes(payload);
                          }
                          return new AutoValue_BigQueryStorageAPIStreamingIT_Value(
                              l, ByteBuffer.wrap(payload));
                        }));

    // Write results using Vortex.
    values.apply(
        "writeVortex",
        BigQueryIO.<Value>write()
            .useBeamSchema()
            .to(options.getTargetTable())
            .withMethod(Write.Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withNumStorageWriteApiStreams(options.getNumShards())
            .withTriggeringFrequency(Duration.standardSeconds(options.getTriggerFrequencySec())));

    p.run();
  }
}
