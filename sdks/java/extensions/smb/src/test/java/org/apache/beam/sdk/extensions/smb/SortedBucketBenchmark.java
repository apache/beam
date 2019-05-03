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
package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonFileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * SMB benchmark.
 */
public class SortedBucketBenchmark {

  /**
   * SMB sink Pipeline options.
   */
  public interface SinkOptions extends PipelineOptions {
    String getAvroDestination();

    void setAvroDestination(String value);

    String getJsonDestination();

    void setJsonDestination(String value);
  }

  public static void main(String[] args) throws CannotProvideCoderException {
    SinkOptions sinkOptions = PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);
    Pipeline pipeline = Pipeline.create(sinkOptions);

    int numKeys = 10 * 1000 * 1000;
    int maxRecordsPerKey = 500;
    int numBuckets = 128;

    PCollection<AvroGeneratedUser> avroData =
        pipeline
            .apply(GenerateSequence.from(0).to(numKeys))
            .apply(
                FlatMapElements.into(TypeDescriptor.of(AvroGeneratedUser.class))
                    .via(
                        i -> {
                          String userId = String.format("user-%08d", i);
                          return IntStream.range(0, new Random().nextInt(maxRecordsPerKey) + 1)
                              .boxed()
                              .map(
                                  j ->
                                      AvroGeneratedUser.newBuilder()
                                          .setName(userId)
                                          .setFavoriteNumber(j)
                                          .setFavoriteColor(String.format("color-%08d", j))
                                          .build())
                              .collect(Collectors.toList());
                        }));

    PCollection<TableRow> jsonData =
        pipeline
            .apply(GenerateSequence.from(0).to(numKeys))
            .apply(
                FlatMapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(
                        i -> {
                          String userId = String.format("user-%08d", i);
                          return IntStream.range(0, new Random().nextInt(maxRecordsPerKey / 10) + 1)
                              .boxed()
                              .map(
                                  j ->
                                      new TableRow()
                                          .set("user", userId)
                                          .set("favoritePlace", String.format("place-%08d", j)))
                              .collect(Collectors.toList());
                        }))
            .setCoder(TableRowJsonCoder.of());

    ResourceId tempDirectory = FileSystems.matchNewResource(sinkOptions.getTempLocation(), true);

    AvroBucketMetadata<CharSequence, AvroGeneratedUser> avroMetadata =
        new AvroBucketMetadata<>(
            numBuckets, CharSequence.class, BucketMetadata.HashType.MURMUR3_32, "name");
    SMBFilenamePolicy avroPolicy =
        new SMBFilenamePolicy(
            FileSystems.matchNewResource(sinkOptions.getAvroDestination(), true), ".avro");
    AvroFileOperations<AvroGeneratedUser> avroOps =
        new AvroFileOperations<>(AvroGeneratedUser.class, AvroGeneratedUser.getClassSchema());
    SortedBucketSink<CharSequence, AvroGeneratedUser> avroSink =
        new SortedBucketSink<>(avroMetadata, avroPolicy, avroOps::createWriter, tempDirectory);

    avroData.apply(avroSink);

    JsonBucketMetadata<String> jsonMetadata =
        new JsonBucketMetadata<>(
            numBuckets, String.class, BucketMetadata.HashType.MURMUR3_32, "user");
    SMBFilenamePolicy jsonPolicy =
        new SMBFilenamePolicy(
            FileSystems.matchNewResource(sinkOptions.getJsonDestination(), true), ".json");
    SortedBucketSink<String, TableRow> jsonSink =
        new SortedBucketSink<>(
            jsonMetadata, jsonPolicy, () -> new JsonFileOperations().createWriter(), tempDirectory);

    jsonData.apply(jsonSink);

    pipeline.run();
  }
}
