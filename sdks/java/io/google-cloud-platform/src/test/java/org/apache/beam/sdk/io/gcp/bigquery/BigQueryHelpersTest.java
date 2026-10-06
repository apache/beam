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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.PendingJob;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.PendingJobManager;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryHelpers}. */
@RunWith(JUnit4.class)
public class BigQueryHelpersTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTablesspecParsingLegacySql() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:data_set.table_name");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTablesspecParsingStandardSql() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project.data_set.table_name");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableUrnParsing() {
    TableReference ref =
        BigQueryHelpers.parseTableUrn("projects/my-project/datasets/data_set/tables/table_name");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_validPatterns() {
    BigQueryHelpers.parseTableSpec("a123-456:foo_bar.d");
    BigQueryHelpers.parseTableSpec("a123-456:foo_bar.ग्राहक");
    BigQueryHelpers.parseTableSpec("a12345:b.c");
    BigQueryHelpers.parseTableSpec("a1:b.c");
    BigQueryHelpers.parseTableSpec("b12345.c");
  }

  @Test
  public void testTableParsing_noProjectId() {
    TableReference ref = BigQueryHelpers.parseTableSpec("data_set.table_name");
    assertEquals(null, ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_lakehouseCatalogDotted() {
    // 4-part Lakehouse runtime catalog reference: project.catalog.namespace.table. The
    // catalog+namespace form a composite dataset id, including when the catalog name uses the
    // GCS-bucket charset (lowercase, digits, dashes).
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project.my-bucket-catalog.my_ns.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("my-bucket-catalog.my_ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_lakehouseCatalogColon() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:my-catalog.my_ns.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("my-catalog.my_ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_lakehouseCatalogNoProject() {
    // Dataset ids may contain characters that project ids may not (e.g. '_'), in which case the
    // whole prefix is the (composite) dataset id.
    TableReference ref = BigQueryHelpers.parseTableSpec("my_catalog.my_ns.tbl");
    assertEquals(null, ref.getProjectId());
    assertEquals("my_catalog.my_ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_multiLevelNamespace() {
    // More than four segments: everything between the project and the table becomes the dataset.
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project.cat.ns1.ns2.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("cat.ns1.ns2", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_lakehouseWithPartitionDecorator() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project.my-catalog.ns.tbl$20260101");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("my-catalog.ns", ref.getDatasetId());
    assertEquals("tbl$20260101", ref.getTableId());
  }

  @Test
  public void testTableParsing_domainScopedProjectPreserved() {
    // Legacy domain-scoped projects keep their historical binding.
    TableReference ref = BigQueryHelpers.parseTableSpec("example.com:project:data_set.tbl");
    assertEquals("example.com:project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());

    ref = BigQueryHelpers.parseTableSpec("example.com:project.data_set.tbl");
    assertEquals("example.com:project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_domainScopedProjectWithCompositeDataset() {
    // With two colons, the last colon is an explicit project terminator, so a domain-scoped
    // project can address a Lakehouse catalog table: the remainder binds as a composite
    // dataset.
    TableReference ref = BigQueryHelpers.parseTableSpec("example.com:project:cat.ns.tbl");
    assertEquals("example.com:project", ref.getProjectId());
    assertEquals("cat.ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());

    // The dotted spelling binds consistently: the first segment after the colon completes the
    // domain-scoped project id; further middle segments form the composite dataset. (Project
    // names cannot contain dots, so the pre-fix greedy binding of this string was invalid.)
    ref = BigQueryHelpers.parseTableSpec("example.com:project.cat.ns.tbl");
    assertEquals("example.com:project", ref.getProjectId());
    assertEquals("cat.ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_partitionDecoratorColonForm() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:my-catalog.ns.tbl$20260101");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("my-catalog.ns", ref.getDatasetId());
    assertEquals("tbl$20260101", ref.getTableId());
  }

  @Test
  public void testTableParsing_tableIdSpecialCharacters() {
    // Table ids may contain spaces, '@', '$', dashes, and unicode letters, none of which
    // affect segment binding (only '.' and ':' are structural).
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project.data_set.my table@x-1");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("my table@x-1", ref.getTableId());

    ref = BigQueryHelpers.parseTableSpec("my-project.my-catalog.ns.ग्राहक");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("my-catalog.ns", ref.getDatasetId());
    assertEquals("ग्राहक", ref.getTableId());
  }

  @Test
  public void testTableParsing_colonFormMultiLevelNamespace() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:cat.ns1.ns2.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("cat.ns1.ns2", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_domainScopedMultiLevelNamespace() {
    // Single-colon domain-scoped spelling with a multi-level composite dataset: the first
    // segment after the colon completes the project id; everything else up to the table binds
    // as the dataset.
    TableReference ref = BigQueryHelpers.parseTableSpec("example.com:proj.cat.ns1.ns2.tbl");
    assertEquals("example.com:proj", ref.getProjectId());
    assertEquals("cat.ns1.ns2", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_consecutiveDotsPreservedInDataset() {
    // Dataset content is preserved verbatim; degenerate consecutive dots are not collapsed
    // (the service rejects the invalid dataset id; the parser must not mangle it).
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:a..b.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("a..b", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_trailingDotStaysOnLegacyBinding() {
    // Degenerate trailing-dot specs pass the character-set gate with the dot inside the dataset
    // id. They must keep their historical binding; the parser must never produce an empty
    // dataset id.
    TableReference ref = BigQueryHelpers.parseTableSpec("pp..t");
    assertEquals(null, ref.getProjectId());
    assertEquals("pp.", ref.getDatasetId());
    assertEquals("t", ref.getTableId());

    ref = BigQueryHelpers.parseTableSpec("google.com:proj..t");
    assertEquals("google.com", ref.getProjectId());
    assertEquals("proj.", ref.getDatasetId());
    assertEquals("t", ref.getTableId());
  }

  @Test
  public void testTableParsing_absorptionRequiresProjectCharset() {
    // The segment absorbed into a domain-scoped project must be legal in the project charset,
    // exactly what the historical greedy regex could absorb.
    // '_' and uppercase were never absorbable:
    TableReference ref = BigQueryHelpers.parseTableSpec("google.com:My_Cat.n.x.t");
    assertEquals("google.com", ref.getProjectId());
    assertEquals("My_Cat.n.x", ref.getDatasetId());
    assertEquals("t", ref.getTableId());

    // ...but a single-character segment was:
    ref = BigQueryHelpers.parseTableSpec("example.com:a.b.t");
    assertEquals("example.com:a", ref.getProjectId());
    assertEquals("b", ref.getDatasetId());
    assertEquals("t", ref.getTableId());
  }

  @Test
  public void testTableParsing_nonProjectLeadingSegmentsFoldIntoDataset() {
    // Leading segments that cannot be project ids (uppercase, '_', too short, leading '_')
    // fold into the (composite) dataset with a null project, regardless of depth.
    String[][] cases = {
      {"My-Ds.ns.t", "My-Ds.ns"},
      {"my_cat.a.b.t", "my_cat.a.b"},
      {"a.c.n.t", "a.c.n"},
      {"_x.a.b.c.t", "_x.a.b.c"},
    };
    for (String[] c : cases) {
      TableReference ref = BigQueryHelpers.parseTableSpec(c[0]);
      assertEquals(c[0], null, ref.getProjectId());
      assertEquals(c[0], c[1], ref.getDatasetId());
      assertEquals(c[0], "t", ref.getTableId());
    }
  }

  @Test
  public void testTableParsing_domainScopedNoAbsorptionWithoutDottedDataset() {
    // Dot-less post-colon remainder: nothing to absorb; the pre-colon text alone is the project.
    TableReference ref = BigQueryHelpers.parseTableSpec("google.com:data_set.tbl");
    assertEquals("google.com", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_twoColonsDotlessDomain() {
    // The explicit-terminator rule does not require a dotted (domain-like) project.
    TableReference ref = BigQueryHelpers.parseTableSpec("p1:x2:data_set.tbl");
    assertEquals("p1:x2", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());

    ref = BigQueryHelpers.parseTableSpec("p1:x2:cat.ns.tbl");
    assertEquals("p1:x2", ref.getProjectId());
    assertEquals("cat.ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_uppercaseCatalogUnderNormalProject() {
    TableReference ref = BigQueryHelpers.parseTableSpec("my-project:My_Cat.ns.tbl");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("My_Cat.ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsingError_rejectedForms() {
    String[] rejected = {
      "p1:t2", // colon form requires dataset.table
      "ds.", // empty table
      ".t", // empty dataset
      "p1:d2.t3.", // trailing separator
      "p1::ds.t", // colon cannot follow the separator colon
      "MyProj:ds.t", // uppercase cannot precede a colon
      "d1:d2:d3:data_set.tbl", // project ids contain at most one colon, so two is the maximum
    };
    for (String spec : rejected) {
      try {
        BigQueryHelpers.parseTableSpec(spec);
        fail("Expected IllegalArgumentException for spec: " + spec);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testParseTableSpecIdempotentThroughToTableSpec() {
    // parse(toTableSpec(parse(s))) == parse(s) for every accepted spec, the string-level
    // consequence of the round-trip fixed point, covering all rebinding families.
    String[] specs = {
      "my-project:data_set.tbl",
      "my-project.cat.ns.tbl",
      "my-project:cat.ns.tbl",
      "my-project:cat.ns1.ns2.tbl",
      "example.com:proj:cat.ns.tbl",
      "example.com:proj.cat.ns.tbl",
      "my_cat.ns.tbl",
      "my-project:My_Cat.ns.tbl",
      "my-project:a..b.tbl",
      "pp..t",
      "google.com:proj..t",
      "my-project:data_set.tbl$20260101",
    };
    for (String spec : specs) {
      TableReference once = BigQueryHelpers.parseTableSpec(spec);
      TableReference twice = BigQueryHelpers.parseTableSpec(BigQueryHelpers.toTableSpec(once));
      assertEquals(spec, once.getProjectId(), twice.getProjectId());
      assertEquals(spec, once.getDatasetId(), twice.getDatasetId());
      assertEquals(spec, once.getTableId(), twice.getTableId());
    }
  }

  @Test
  public void testTableParsingError_colonIsNotATableSeparator() {
    // The dataset/table separator must be a dot; a spec with no dot at all is rejected even
    // if it contains colons.
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("my-project:data_set:tbl");
  }

  @Test
  public void testTableParsing_projectlessCatalogSpecIsAmbiguous() {
    // A project-less catalog reference is indistinguishable from project.dataset.table when the
    // catalog name fits the project-id charset: the project interpretation wins. Users must
    // write the full 4-part name (or use a TableReference) for such catalogs; only catalog
    // names that are illegal as project ids (e.g. containing '_') parse as a composite dataset
    // with the project left to be defaulted.
    TableReference ref = BigQueryHelpers.parseTableSpec("my-bucket-catalog.my_ns.tbl");
    assertEquals("my-bucket-catalog", ref.getProjectId());
    assertEquals("my_ns", ref.getDatasetId());
    assertEquals("tbl", ref.getTableId());
  }

  @Test
  public void testTableParsing_informationSchema() {
    // INFORMATION_SCHEMA views ride along with the multi-segment rule: the pseudo-schema folds
    // into the dataset id. (Neither the Storage Read API nor extract jobs support reading
    // INFORMATION_SCHEMA views, so this reference form never reaches a table API; pinning the
    // binding here documents that it at least keeps a valid, undotted project id.)
    TableReference ref =
        BigQueryHelpers.parseTableSpec("my-project.data_set.INFORMATION_SCHEMA.TABLES");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set.INFORMATION_SCHEMA", ref.getDatasetId());
    assertEquals("TABLES", ref.getTableId());
  }

  @Test
  public void testTableParsing_shortFirstSegmentIsNotAProject() {
    // A single character cannot be a project id, so the prefix folds into the dataset id
    // (historical behavior).
    TableReference ref = BigQueryHelpers.parseTableSpec("a.b.c");
    assertEquals(null, ref.getProjectId());
    assertEquals("a.b", ref.getDatasetId());
    assertEquals("c", ref.getTableId());
  }

  @Test
  public void testToTableSpecParseTableSpecRoundTrip() {
    List<TableReference> refs =
        Arrays.asList(
            new TableReference()
                .setProjectId("my-project")
                .setDatasetId("data_set")
                .setTableId("tbl"),
            new TableReference()
                .setProjectId("my-project")
                .setDatasetId("my-catalog.my_ns")
                .setTableId("tbl"),
            new TableReference()
                .setProjectId("example.com:project")
                .setDatasetId("data_set")
                .setTableId("tbl"),
            new TableReference()
                .setProjectId("example.com:project")
                .setDatasetId("my-catalog.my_ns")
                .setTableId("tbl"),
            new TableReference()
                .setProjectId("my-project")
                .setDatasetId("cat.ns1.ns2")
                .setTableId("tbl"),
            new TableReference()
                .setProjectId("my-project")
                .setDatasetId("my-catalog.ns")
                .setTableId("tbl$20260101"),
            new TableReference().setDatasetId("data_set").setTableId("tbl"));
    for (TableReference ref : refs) {
      TableReference reparsed = BigQueryHelpers.parseTableSpec(BigQueryHelpers.toTableSpec(ref));
      assertEquals(BigQueryHelpers.toTableSpec(ref), ref.getProjectId(), reparsed.getProjectId());
      assertEquals(BigQueryHelpers.toTableSpec(ref), ref.getDatasetId(), reparsed.getDatasetId());
      assertEquals(BigQueryHelpers.toTableSpec(ref), ref.getTableId(), reparsed.getTableId());
    }
  }

  @Test
  public void testTableParsingError0() {
    String expectedMessage =
        "Table specification [foo_bar_baz] is not in one of the expected formats ("
            + " [project_id]:[dataset_id].[table_id],"
            + " [project_id].[dataset_id].[table_id],"
            + " [dataset_id].[table_id],"
            + " [project_id]:[catalog_id].[namespace_id].[table_id],"
            + " [project_id].[catalog_id].[namespace_id].[table_id])";

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(expectedMessage);
    BigQueryHelpers.parseTableSpec("foo_bar_baz");
  }

  @Test
  public void testTableParsingError() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("0123456:foo.bar");
  }

  @Test
  public void testTableParsingError_2() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("myproject:.bar");
  }

  @Test
  public void testTableParsingError_3() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec(":a.b");
  }

  @Test
  public void testTableParsingError_slash() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("a\\b12345:c.d");
  }

  @Test
  public void testTableDecoratorStripping() {
    assertEquals(
        "project:dataset.table",
        BigQueryHelpers.stripPartitionDecorator("project:dataset.table$20171127"));
    assertEquals(
        "project:dataset.table", BigQueryHelpers.stripPartitionDecorator("project:dataset.table"));
  }

  // Test that BigQuery's special null placeholder objects can be encoded.
  @Test
  public void testCoder_nullCell() throws CoderException {
    TableRow row = new TableRow();
    row.set("temperature", Data.nullOf(Object.class));
    row.set("max_temperature", Data.nullOf(Object.class));

    byte[] bytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), row);

    TableRow newRow = CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), bytes);
    byte[] newBytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), newRow);

    Assert.assertArrayEquals(bytes, newBytes);
  }

  @Test
  public void testShardedKeyCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(ShardedKeyCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testTableRowInfoCoderSerializable() {
    CoderProperties.coderSerializable(TableRowInfoCoder.of(TableRowJsonCoder.of()));
  }

  @Test
  public void testComplexCoderSerializable() {
    CoderProperties.coderSerializable(
        WindowedValues.getFullCoder(
            KvCoder.of(
                ShardedKeyCoder.of(StringUtf8Coder.of()),
                TableRowInfoCoder.of(TableRowJsonCoder.of())),
            IntervalWindow.getCoder()));
  }

  @Test
  public void testPendingJobManager() throws Exception {
    PendingJobManager jobManager =
        new PendingJobManager(
            BackOffAdapter.toGcpBackOff(
                FluentBackoff.DEFAULT
                    .withMaxRetries(Integer.MAX_VALUE)
                    .withInitialBackoff(Duration.millis(10))
                    .withMaxBackoff(Duration.millis(10))
                    .backoff()));

    Set<String> succeeded = Sets.newHashSet();
    for (int i = 0; i < 5; i++) {
      Job currentJob = new Job();
      currentJob.setKind(" bigquery#job");
      PendingJob pendingJob =
          new PendingJob(
              retryId -> {
                if (new Random().nextInt(2) == 0) {
                  throw new RuntimeException("Failing to start.");
                }
                currentJob.setJobReference(
                    new JobReference()
                        .setProjectId("")
                        .setLocation("")
                        .setJobId(retryId.getJobId()));
                return null;
              },
              retryId -> {
                if (retryId.getRetryIndex() < 5) {
                  currentJob.setStatus(new JobStatus().setErrorResult(new ErrorProto()));
                } else {
                  currentJob.setStatus(new JobStatus().setErrorResult(null));
                }
                return currentJob;
              },
              retryId -> {
                if (retryId.getJobId().equals(currentJob.getJobReference().getJobId())) {
                  return currentJob;
                } else {
                  return null;
                }
              },
              100,
              "JOB_" + i);
      jobManager.addPendingJob(
          pendingJob,
          j -> {
            succeeded.add(j.currentJobId.getJobId());
            return null;
          });
    }

    jobManager.waitForDone();
    Set<String> expectedJobs =
        ImmutableSet.of("JOB_0-5", "JOB_1-5", "JOB_2-5", "JOB_3-5", "JOB_4-5");
    assertEquals(expectedJobs, succeeded);
  }

  @Test
  public void testCreateTempTableReference() {
    String projectId = "this-is-my-project";
    String jobUuid = "this-is-my-job";
    TableReference noDataset =
        BigQueryResourceNaming.createTempTableReference(projectId, jobUuid, Optional.empty());

    assertEquals(noDataset.getProjectId(), projectId);
    assertEquals(noDataset.getDatasetId(), "temp_dataset_" + jobUuid);
    assertEquals(noDataset.getTableId(), "temp_table_" + jobUuid);

    Optional<String> dataset = Optional.ofNullable("my-tmp-dataset");
    TableReference tempTableReference =
        BigQueryResourceNaming.createTempTableReference(projectId, jobUuid, dataset);

    assertEquals(tempTableReference.getProjectId(), noDataset.getProjectId());
    assertEquals(tempTableReference.getDatasetId(), dataset.get());
    assertEquals(tempTableReference.getTableId(), noDataset.getTableId());

    assertEquals(dataset.get(), noDataset.setDatasetId(dataset.get()).getDatasetId());
  }

  @Test
  public void testClusteringJsonConversion() {
    Clustering clustering =
        new Clustering().setFields(Arrays.asList("column1", "column2", "column3"));
    String jsonClusteringFields = "[\"column1\", \"column2\", \"column3\"]";

    assertEquals(clustering, BigQueryHelpers.clusteringFromJsonFields(jsonClusteringFields));
  }
}
