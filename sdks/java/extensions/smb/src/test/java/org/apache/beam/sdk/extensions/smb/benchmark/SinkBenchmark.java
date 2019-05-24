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
package org.apache.beam.sdk.extensions.smb.benchmark;

import static org.apache.beam.sdk.coders.Coder.NonDeterministicException;

import com.google.api.services.bigquery.model.TableRow;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonFileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Benchmark of {@link SortedBucketSink}.
 *
 * <p>Generates 2 collections with numKeys unique keys each, the Avro one with [0, maxRecordsPerKey]
 * values per key, uniformly distributed, and the JSON one [0, maxRecordsPerKey / 10] values per
 * key.
 */
public class SinkBenchmark {

  /** SinkOptions. */
  public interface SinkOptions extends PipelineOptions {
    String getAvroDestination();

    void setAvroDestination(String value);

    String getJsonDestination();

    void setJsonDestination(String value);

    @Default.Integer(10000000)
    int getNumKeys();

    void setNumKeys(int value);

    @Default.Integer(500)
    int getMaxRecordsPerKey();

    void setMaxRecordsPerKey(int value);

    @Default.Integer(10)
    int getAvroNumShards();

    void setAvroNumShards(int value);

    @Default.Integer(128)
    int getAvroNumBuckets();

    void setAvroNumBuckets(int value);

    @Default.Integer(10)
    int getJsonNumShards();

    void setJsonNumShards(int value);

    @Default.Integer(128)
    int getJsonNumBuckets();

    void setJsonNumBuckets(int value);
  }

  public static void main(String[] args)
      throws CannotProvideCoderException, NonDeterministicException {
    final SinkOptions sinkOptions = PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);
    final Pipeline pipeline = Pipeline.create(sinkOptions);

    final int numKeys = sinkOptions.getNumKeys();
    final int maxRecordsPerKey = sinkOptions.getMaxRecordsPerKey();
    final int avroNumBuckets = sinkOptions.getAvroNumBuckets();
    final int avroNumShards = sinkOptions.getAvroNumShards();

    final PCollection<AvroGeneratedUser> avroData =
        pipeline
            .apply(GenerateSequence.from(0).to(numKeys))
            .apply(
                FlatMapElements.into(TypeDescriptor.of(AvroGeneratedUser.class))
                    .via(
                        i ->
                            IntStream.rangeClosed(
                                    0, ThreadLocalRandom.current().nextInt(maxRecordsPerKey))
                                .boxed()
                                .map(
                                    j ->
                                        AvroGeneratedUser.newBuilder()
                                            .setName(String.format("user-%08d", i))
                                            .setFavoriteNumber(j)
                                            .setFavoriteColor(String.format("color-%08d", j))
                                            .build())
                                .collect(Collectors.toList())));

    final int jsonNumBuckets = sinkOptions.getJsonNumBuckets();
    final int jsonNumShards = sinkOptions.getJsonNumShards();

    final PCollection<TableRow> jsonData =
        pipeline
            .apply(GenerateSequence.from(0).to(numKeys))
            .apply(
                FlatMapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(
                        i ->
                            IntStream.rangeClosed(
                                    0, ThreadLocalRandom.current().nextInt(maxRecordsPerKey / 10))
                                .boxed()
                                .map(
                                    j ->
                                        new TableRow()
                                            .set("user", String.format("user-%08d", i))
                                            .set("favoritePlace", String.format("place-%08d", j)))
                                .collect(Collectors.toList())))
            .setCoder(TableRowJsonCoder.of());

    final ResourceId tempDirectory =
        FileSystems.matchNewResource(sinkOptions.getTempLocation(), true);

    final AvroBucketMetadata<CharSequence, AvroGeneratedUser> avroMetadata =
        new AvroBucketMetadata<>(
            avroNumBuckets,
            avroNumShards,
            CharSequence.class,
            BucketMetadata.HashType.MURMUR3_32,
            "name");
    final SMBFilenamePolicy avroPolicy =
        new SMBFilenamePolicy(
            FileSystems.matchNewResource(sinkOptions.getAvroDestination(), true), ".avro");
    final AvroFileOperations<AvroGeneratedUser> avroOps =
        AvroFileOperations.of(AvroGeneratedUser.class);
    final SortedBucketSink<CharSequence, AvroGeneratedUser> avroSink =
        new SortedBucketSink<>(avroMetadata, avroPolicy, avroOps::createWriter, tempDirectory);

    avroData.apply(avroSink);

    final JsonBucketMetadata<String> jsonMetadata =
        new JsonBucketMetadata<>(
            jsonNumBuckets,
            jsonNumShards,
            String.class,
            BucketMetadata.HashType.MURMUR3_32,
            "user");
    final SMBFilenamePolicy jsonPolicy =
        new SMBFilenamePolicy(
            FileSystems.matchNewResource(sinkOptions.getJsonDestination(), true), ".json");
    JsonFileOperations jsonOps = new JsonFileOperations();
    final SortedBucketSink<String, TableRow> jsonSink =
        new SortedBucketSink<>(jsonMetadata, jsonPolicy, jsonOps::createWriter, tempDirectory);

    jsonData.apply(jsonSink);

    long startTime = System.currentTimeMillis();
    State state = pipeline.run().waitUntilFinish();
    System.out.println(
        String.format(
            "SinkBenchmark finished with state %s in %d ms",
            state, System.currentTimeMillis() - startTime));
  }
}
