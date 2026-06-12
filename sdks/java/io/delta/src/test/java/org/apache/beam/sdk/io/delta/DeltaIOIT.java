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
package org.apache.beam.sdk.io.delta;

import static org.junit.Assert.assertNotNull;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link DeltaIO}. */
@RunWith(JUnit4.class)
public class DeltaIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaIOIT.class);

  private static final String DEFAULT_BUCKET = "apache-beam-testing-delta-lake";
  @Rule public final TestPipeline readPipeline = TestPipeline.create();
  @Rule public final TestName testName = new TestName();

  private String bucket;
  private String repoPath;
  private String repoPrefix;
  private Storage storage;

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  private static final List<Row> TEST_ROWS =
      IntStream.range(0, 100)
          .mapToObj(i -> Row.withSchema(ROW_SCHEMA).addValues(i, "name_" + i).build())
          .collect(Collectors.toList());

  @Before
  public void setup() throws Exception {
    storage = StorageOptions.newBuilder().build().getService();
    long salt = System.currentTimeMillis();

    String tempLocation = readPipeline.getOptions().getTempLocation();
    if (tempLocation != null && tempLocation.startsWith("gs://")) {
      org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath gcsPath = org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath
          .fromUri(tempLocation);
      bucket = gcsPath.getBucket();
      repoPrefix = gcsPath.getObject() + "/delta_io_it/" + testName.getMethodName() + "-" + salt;
    } else {
      bucket = DEFAULT_BUCKET;
      repoPrefix = "delta_io_it/" + testName.getMethodName() + "-" + salt;
    }
    repoPath = "gs://" + bucket + "/" + repoPrefix;

    LOG.info("Generating Delta Lake repository at {}", repoPath);

    // 1. Write Parquet file using a direct local pipeline
    DirectOptions setupOptions = PipelineOptionsFactory.as(DirectOptions.class);
    setupOptions.setRunner(DirectRunner.class);
    setupOptions.setBlockOnRun(true);
    Pipeline setupPipeline = Pipeline.create(setupOptions);

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(ROW_SCHEMA);
    setupPipeline
        .apply(Create.of(TEST_ROWS).withRowSchema(ROW_SCHEMA))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(avroSchema))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(avroSchema))
                .to(repoPath + "/")
                .withNaming(
                    (BoundedWindow window,
                        PaneInfo paneInfo,
                        int numShards,
                        int shardIndex,
                        Compression compression) -> "part-00000.parquet"));
    setupPipeline.run().waitUntilFinish();

    // 2. Find written Parquet file to inspect its size
    BlobId parquetBlobId = BlobId.of(bucket, repoPrefix + "/part-00000.parquet");
    Blob parquetBlob = storage.get(parquetBlobId);
    assertNotNull("Parquet file not found on GCS: " + parquetBlobId, parquetBlob);
    long fileSize = parquetBlob.getSize();

    // 3. Create the Delta log commit file
    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\""
            + salt
            + "\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + fileSize
            + ",\"modificationTime\":123456789,\"dataChange\":true}}";

    BlobId commitBlobId = BlobId.of(bucket, repoPrefix + "/_delta_log/00000000000000000000.json");
    BlobInfo commitBlobInfo =
        BlobInfo.newBuilder(commitBlobId).setContentType("application/json").build();
    storage.create(commitBlobInfo, commitContent.getBytes(StandardCharsets.UTF_8));
    LOG.info("Successfully generated Delta Lake repository");
  }

  @After
  public void teardown() {
    if (storage == null) {
      return;
    }
    LOG.info("Cleaning up Delta Lake repository at {}", repoPath);
    try {
      Iterable<Blob> blobs =
          storage.list(bucket, Storage.BlobListOption.prefix(repoPrefix)).getValues();
      blobs.forEach(b -> storage.delete(b.getBlobId()));
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS repository at {}", repoPath, e);
    }
  }

  @Test
  public void testReadDeltaLakeTable() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hadoopConfig.put(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String project = readPipeline
        .getOptions()
        .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
        .getProject();
    if (project != null) {
      hadoopConfig.put("fs.gs.project.id", project);
    }

    PCollection<Row> output =
        readPipeline
            .apply(
                Managed.read(Managed.DELTA_LAKE)
                    .withConfig(ImmutableMap.of("table", repoPath, "hadoop_config", hadoopConfig)))
            .getSinglePCollection();

    PAssert.that(output).containsInAnyOrder(TEST_ROWS);
    readPipeline.run().waitUntilFinish();
  }
}
