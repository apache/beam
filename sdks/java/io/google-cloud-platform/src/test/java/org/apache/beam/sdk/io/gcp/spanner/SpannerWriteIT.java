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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Sink. */
@RunWith(JUnit4.class)
public class SpannerWriteIT {

  private static final int MAX_DB_NAME_LENGTH = 30;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends TestPipelineOptions {
    @Description("Project that hosts Spanner instance")
    @Nullable
    String getInstanceProjectId();

    void setInstanceProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("beam-test")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID prefix to write to in Spanner")
    @Default.String("beam-testdb")
    String getDatabaseIdPrefix();

    void setDatabaseIdPrefix(String value);

    @Description("Table name")
    @Default.String("users")
    String getTable();

    void setTable(String value);
  }

  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private SpannerTestPipelineOptions options;
  private String databaseName;
  private String project;

  @Before
  public void setUp() throws Exception {
    PipelineOptionsFactory.register(SpannerTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SpannerTestPipelineOptions.class);

    project = options.getInstanceProjectId();
    if (project == null) {
      project = options.as(GcpOptions.class).getProject();
    }

    spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();

    databaseName = generateDatabaseName();

    databaseAdminClient = spanner.getDatabaseAdminClient();

    // Delete database if exists.
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);

    OperationFuture<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(
            options.getInstanceId(),
            databaseName,
            Collections.singleton(
                "CREATE TABLE "
                    + options.getTable()
                    + " ("
                    + "  Key           INT64,"
                    + "  Value         STRING(MAX) NOT NULL,"
                    + ") PRIMARY KEY (Key)"));
    op.get();
  }

  private String generateDatabaseName() {
    String random =
        RandomUtils.randomAlphaNumeric(
            MAX_DB_NAME_LENGTH - 1 - options.getDatabaseIdPrefix().length());
    return options.getDatabaseIdPrefix() + "-" + random;
  }

  @Test
  public void testWrite() throws Exception {
    int numRecords = 100;
    p.apply(GenerateSequence.from(0).to(numRecords))
        .apply(ParDo.of(new GenerateMutations(options.getTable())))
        .apply(
            SpannerIO.write()
                .withProjectId(project)
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(databaseName));

    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
    assertThat(countNumberOfRecords(), equalTo((long) numRecords));
  }

  @Test
  public void testSequentialWrite() throws Exception {
    int numRecords = 100;

    SpannerWriteResult stepOne =
        p.apply("first step", GenerateSequence.from(0).to(numRecords))
            .apply(ParDo.of(new GenerateMutations(options.getTable())))
            .apply(
                SpannerIO.write()
                    .withProjectId(project)
                    .withInstanceId(options.getInstanceId())
                    .withDatabaseId(databaseName));

    p.apply("second step", GenerateSequence.from(numRecords).to(2 * numRecords))
        .apply("Gen mutations", ParDo.of(new GenerateMutations(options.getTable())))
        .apply(Wait.on(stepOne.getOutput()))
        .apply(
            "write to table2",
            SpannerIO.write()
                .withProjectId(project)
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(databaseName));

    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
    assertThat(countNumberOfRecords(), equalTo(2L * numRecords));
  }

  @Test
  public void testReportFailures() throws Exception {
    int numRecords = 100;
    p.apply(GenerateSequence.from(0).to(2 * numRecords))
        .apply(ParDo.of(new GenerateMutations(options.getTable(), new DivBy2())))
        .apply(
            SpannerIO.write()
                .withProjectId(project)
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(databaseName)
                .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES));

    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
    assertThat(countNumberOfRecords(), equalTo((long) numRecords));
  }

  @Test
  public void testFailFast() throws Exception {
    thrown.expect(new StackTraceContainsString("SpannerException"));
    thrown.expect(new StackTraceContainsString("Value must not be NULL in table users"));
    int numRecords = 100;
    p.apply(GenerateSequence.from(0).to(2 * numRecords))
        .apply(ParDo.of(new GenerateMutations(options.getTable(), new DivBy2())))
        .apply(
            SpannerIO.write()
                .withProjectId(project)
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(databaseName));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  @After
  public void tearDown() throws Exception {
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);
    spanner.close();
  }

  private static class GenerateMutations extends DoFn<Long, Mutation> {
    private final String table;
    private final int valueSize = 100;
    private final Predicate<Long> injectError;

    public GenerateMutations(String table, Predicate<Long> injectError) {
      this.table = table;
      this.injectError = injectError;
    }

    public GenerateMutations(String table) {
      this(table, Predicates.<Long>alwaysFalse());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
      Long key = c.element();
      builder.set("Key").to(key);
      String value = injectError.apply(key) ? null : RandomUtils.randomAlphaNumeric(valueSize);
      builder.set("Value").to(value);
      Mutation mutation = builder.build();
      c.output(mutation);
    }
  }

  private long countNumberOfRecords() {
    ResultSet resultSet =
        spanner
            .getDatabaseClient(DatabaseId.of(project, options.getInstanceId(), databaseName))
            .singleUse()
            .executeQuery(Statement.of("SELECT COUNT(*) FROM " + options.getTable()));
    assertThat(resultSet.next(), is(true));
    long result = resultSet.getLong(0);
    assertThat(resultSet.next(), is(false));
    return result;
  }

  private static class DivBy2 implements Predicate<Long>, Serializable {

    @Override
    public boolean apply(@Nullable Long input) {
      return input % 2 == 0;
    }
  }

  static class StackTraceContainsString extends TypeSafeMatcher<Exception> {

    private String str;

    public StackTraceContainsString(String str) {
      this.str = str;
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
      description.appendText("stack trace contains string '" + str + "'");
    }

    @Override
    protected boolean matchesSafely(Exception e) {
      String stacktrace = Throwables.getStackTraceAsString(e);
      return stacktrace.contains(str);
    }
  }
}
