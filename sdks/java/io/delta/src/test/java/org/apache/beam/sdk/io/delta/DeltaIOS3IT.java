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

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Integration tests for {@link DeltaIO} on AWS S3 using Localstack. */
@RunWith(JUnit4.class)
public class DeltaIOS3IT {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaIOS3IT.class);

  @ClassRule
  public static LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.13.1"))
          .withServices(LocalStackContainer.Service.S3)
          .withLogConsumer(frame -> System.out.print("[LocalStack S3] " + frame.getUtf8String()));

  @Rule public final TestPipeline readPipeline = TestPipeline.create();
  @Rule public final TestName testName = new TestName();

  private String bucketName;
  private String repoPath;
  private Configuration configuration;
  private S3Client s3Client;

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  private static final List<Row> TEST_ROWS =
      IntStream.range(0, 100)
          .mapToObj(i -> Row.withSchema(ROW_SCHEMA).addValues(i, "name_" + i).build())
          .collect(Collectors.toList());

  @Before
  public void setup() throws Exception {
    bucketName = "beam-delta-s3-it-" + System.currentTimeMillis();
    repoPath = "s3a://" + bucketName + "/delta_io_it/" + testName.getMethodName();

    LOG.info("Creating localstack S3 client and bucket: {}", bucketName);
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    s3Client.createBucket(b -> b.bucket(bucketName));

    LOG.info("Generating Delta Lake repository at {}", repoPath);

    configuration = new Configuration();
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
    configuration.set(
        "fs.s3a.endpoint",
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    configuration.set("fs.s3a.access.key", localstack.getAccessKey());
    configuration.set("fs.s3a.secret.key", localstack.getSecretKey());
    configuration.set("fs.s3a.audit.enabled", "false");
    configuration.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

    Engine engine = DefaultEngine.create(configuration);
    Table table = Table.forPath(engine, repoPath);

    StructType deltaSchema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaIOS3IT", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, deltaSchema);
    Transaction txn = txnBuilder.build(engine);
    io.delta.kernel.data.Row txnState = txn.getTransactionState(engine);

    ColumnVector idVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return IntegerType.INTEGER;
          }

          @Override
          public int getSize() {
            return TEST_ROWS.size();
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return TEST_ROWS.get(rowId).getValue("id") == null;
          }

          @Override
          public int getInt(int rowId) {
            return TEST_ROWS.get(rowId).getInt32("id");
          }
        };

    ColumnVector nameVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return TEST_ROWS.size();
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return TEST_ROWS.get(rowId).getValue("name") == null;
          }

          @Override
          public String getString(int rowId) {
            return TEST_ROWS.get(rowId).getString("name");
          }
        };

    ColumnVector[] vectors = new ColumnVector[] {idVector, nameVector};
    ColumnarBatch columnarBatch = new DefaultColumnarBatch(TEST_ROWS.size(), deltaSchema, vectors);
    FilteredColumnarBatch filteredBatch =
        new FilteredColumnarBatch(columnarBatch, Optional.empty());

    CloseableIterator<FilteredColumnarBatch> data =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(
            Collections.singletonList(filteredBatch).iterator());

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(engine, txnState, data, Collections.emptyMap());

    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

    CloseableIterator<DataFileStatus> dataFiles =
        engine
            .getParquetHandler()
            .writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns());

    CloseableIterator<io.delta.kernel.data.Row> dataActions =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);

    CloseableIterable<io.delta.kernel.data.Row> dataActionsIterable =
        CloseableIterable.inMemoryIterable(dataActions);

    TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

    if (commitResult.getVersion() < 0) {
      throw new RuntimeException("Table creation/write failed");
    }

    LOG.info("Successfully generated Delta Lake repository on S3");
  }

  @After
  public void teardown() {
    if (repoPath != null && configuration != null) {
      LOG.info("Cleaning up Delta Lake repository at {}", repoPath);
      try {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(repoPath);
        org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(configuration);
        fs.delete(path, true);
      } catch (Exception e) {
        LOG.warn("Failed to clean up S3 repository at {}", repoPath, e);
      }
    }
    if (s3Client != null && bucketName != null) {
      try {
        s3Client.deleteBucket(b -> b.bucket(bucketName));
      } catch (Exception e) {
        LOG.warn("Failed to delete bucket {}", bucketName, e);
      }
      s3Client.close();
    }
  }

  @Test
  public void testReadDeltaLakeTableS3() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    hadoopConfig.put("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
    hadoopConfig.put(
        "fs.s3a.endpoint",
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    hadoopConfig.put("fs.s3a.access.key", localstack.getAccessKey());
    hadoopConfig.put("fs.s3a.secret.key", localstack.getSecretKey());
    hadoopConfig.put("fs.s3a.audit.enabled", "false");
    hadoopConfig.put(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

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
