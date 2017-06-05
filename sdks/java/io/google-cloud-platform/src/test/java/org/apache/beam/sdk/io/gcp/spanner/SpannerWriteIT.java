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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.util.Collections;
import java.util.UUID;

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/** End-to-end test of Cloud Spanner Sink. */
@RunWith(JUnit4.class)
public class SpannerWriteIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends TestPipelineOptions {
    @Description("Project ID for Spanner")
    @Default.String("apache-beam-testing")
    String getProjectId();
    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("beam-test")
    String getInstanceId();
    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Default.String("beam-testdb")
    String getDatabaseId();
    void setDatabaseId(String value);
  }

  private Spanner spanner;
  private SpannerTestPipelineOptions options;
  private final String tableName = generateTableName();

  @Before
  public void setUp() throws Exception {
    PipelineOptionsFactory.register(SpannerTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SpannerTestPipelineOptions.class);

    spanner = SpannerOptions.newBuilder().setProjectId(options.getProjectId()).build().getService();

    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    databaseAdminClient.getDatabase(options.getInstanceId(), options.getDatabaseId());

    Operation<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            options.getInstanceId(),
            options.getDatabaseId(),
            Collections.singleton(
                "CREATE TABLE "
                    + tableName
                    + " ("
                    + "  Key           INT64,"
                    + "  Value         STRING(MAX),"
                    + ") PRIMARY KEY (Key)"), null);
    op.waitFor();
  }

  @Test
  public void testWrite() throws Exception {
    p.apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new GenerateMutations(tableName)))
        .apply(
            SpannerIO.write()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(options.getDatabaseId()));

    p.run();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of(
                options.getProjectId(), options.getInstanceId(), options.getDatabaseId()));

    ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of("SELECT COUNT(*) FROM " + tableName));
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getLong(0), equalTo(100L));
    assertThat(resultSet.next(), is(false));
  }

  @After
  public void tearDown() throws Exception {
    spanner.closeAsync().get();
  }

  private static class GenerateMutations extends DoFn<Long, Mutation> {
    private final String table;
    private final int valueSize = 100;

    public GenerateMutations(String table) {
      this.table = table;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
      Long key = c.element();
      builder.set("Key").to(key);
      builder.set("Value").to(RandomStringUtils.random(valueSize, true, true));
      Mutation mutation = builder.build();
      c.output(mutation);
    }
  }

  private static String generateTableName() {
    return "test-table-" + UUID.randomUUID().toString();
  }
}
