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
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BigQueryStorageApiExactlyOnceTransform extends PTransform<PBegin, WriteResult> {
  private final BigQueryIO.Write.Method writeMethod;

  public BigQueryStorageApiExactlyOnceTransform(BigQueryIO.Write.Method writeMethod) {
    this.writeMethod = writeMethod;
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Value {
    public abstract long getNumber();
    public abstract @Nullable ByteBuffer getPayload();
  }

  public interface BigQueryStorageApiExactlyOnceOptions
      extends TestPipelineOptions, BigQueryOptions {
    @Default.Long(100000L)
    @Description("The rate at which elements are processed into the sink.")
    Long getNumElementsPerSecond();

    void setNumElementsPerSecond(Long value);

    @Default.Long(-1)
    @Description("Number of elements to ingest. For streaming we currently use -1 to set it to be unbounded.")
    Long getNumElements();

    void setNumElements(Long value);

    @Default.Long(0)
    @Description("If set to 0, then autosharding will be used. If > 0, controls the number of Storage API streams.")
    Long getNumStreams();

    void setNumStreams(Long value);

    @Default.Long(30)
    @Description("Forcefully crash the Storage API sink every N seconds.")
    Long getParDoCrashDuration();

    void setParDoCrashDuration(Long value);

    @Default.String("The destination table to write to.")
    String getDestinationTable();

    void setDestinationTable(String value);

    @Default.Integer(1024)
    @Description("The size of the additional payload to attach to messages.")
    Integer getPayloadSize();
    void setPayloadSize(Integer value);
  }

  private @Nullable ByteBuffer getPayload(int payloadSize) {
    if (payloadSize <= 0) {
      return null;
    }
    byte[] payload = new byte[payloadSize];
    ThreadLocalRandom.current().nextBytes(payload);
    return ByteBuffer.wrap(payload);
  }

  @Override
  public WriteResult expand(PBegin begin) {
    BigQueryStorageApiExactlyOnceOptions options =
        begin.getPipeline().getOptions().as(BigQueryStorageApiExactlyOnceOptions.class);
    boolean isBounded = options.getNumElements() > 0;
    GenerateSequence generate = GenerateSequence.from(0)
            .withRate(options.getNumElementsPerSecond(), Duration.standardSeconds(1));
    if (isBounded) {
      generate = generate.to(options.getNumElements());
    }

    final int payloadSize = options.getPayloadSize();
    PCollection<Value> values =
        begin
            .apply("Generate numbers", generate)
            .apply(
                MapElements.into(TypeDescriptor.of(Value.class))
                    .via(i -> new AutoValue_BigQueryStorageApiExactlyOnceTransform_Value(i, getPayload(payloadSize))));

    BigQueryIO.Write<Value> write =
        BigQueryIO.<Value>write()
            .useBeamSchema()
            .to(options.getDestinationTable())
            .withMethod(writeMethod)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    if (!isBounded && writeMethod != BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE) {
      write = write.withTriggeringFrequency(Duration.standardSeconds(1));
    }
    if (options.getNumStreams() > 0) {
      write = write.withNumStorageWriteApiStreams(options.getNumStorageWriteApiStreams());
    } else if (!isBounded && writeMethod != BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE) {
      write = write.withAutoSharding();
    }
    values.apply("write Storage API", write);

    BigQueryIO.Write<Value> writeGolden =
            BigQueryIO.<Value>write()
                    .useBeamSchema()
                    .to(options.getDestinationTable() + "_golden")
                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    if (!isBounded) {
      writeGolden = writeGolden
              .withAutoSharding()
              .withTriggeringFrequency(Duration.standardSeconds(5));
    }
    // Write out golden values.
    return values.apply("writeGolden", writeGolden);
  }
}
