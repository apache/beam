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

import com.google.api.services.bigquery.model.TableRow;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.JsonSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Default;
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
  public interface SinkOptions extends BenchmarkOptions {
    @Default.Integer(10000000)
    int getNumKeys();

    void setNumKeys(int value);

    @Default.Integer(500)
    int getMaxRecordsPerKey();

    void setMaxRecordsPerKey(int value);

    @Default.Integer(128)
    int getAvroNumBuckets();

    void setAvroNumBuckets(int value);

    @Default.Integer(10)
    int getAvroNumShards();

    void setAvroNumShards(int value);

    @Default.Integer(128)
    int getJsonNumBuckets();

    void setJsonNumBuckets(int value);

    @Default.Integer(10)
    int getJsonNumShards();

    void setJsonNumShards(int value);
  }

  public static void main(String[] args) {
    final SinkOptions options = PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);
    final Pipeline pipeline = Pipeline.create(options);
    write(pipeline);
  }

  static PipelineResult write(Pipeline pipeline) {
    final SinkOptions options = pipeline.getOptions().as(SinkOptions.class);
    final int numKeys = options.getNumKeys();
    final int maxRecordsPerKey = options.getMaxRecordsPerKey();

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

    final AvroSortedBucketIO.Write<CharSequence, AvroGeneratedUser> avroWrite =
        AvroSortedBucketIO.write(CharSequence.class, "name", AvroGeneratedUser.class)
            .to(options.getAvroPath())
            .withTempDirectory(options.getTempLocation())
            .withNumBuckets(options.getAvroNumBuckets())
            .withNumShards(options.getJsonNumShards())
            .withHashType(BucketMetadata.HashType.MURMUR3_32)
            .withSuffix(".avro")
            .withCodec(CodecFactory.snappyCodec());
    avroData.apply(avroWrite);

    final JsonSortedBucketIO.Write<String> jsonWrite =
        JsonSortedBucketIO.write(String.class, "user")
            .to(options.getJsonPath())
            .withTempDirectory(options.getTempLocation())
            .withNumBuckets(options.getJsonNumBuckets())
            .withNumShards(options.getJsonNumShards())
            .withHashType(BucketMetadata.HashType.MURMUR3_32)
            .withSuffix(".json")
            .withCompression(Compression.UNCOMPRESSED);
    jsonData.apply(jsonWrite);

    return pipeline.run();
  }
}
