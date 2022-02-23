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

import com.google.api.services.bigquery.model.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
@SuppressWarnings({
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
/** Unit tests for {@link TableRowToStorageApiProto}. */
public class TableRowToStorageApiProtoIT {

    private static final Logger LOG = LoggerFactory.getLogger(TableRowToStorageApiProtoIT.class);
    private static final BigqueryClient BQ_CLIENT = new BigqueryClient("TableRowToStorageApiProtoIT");

    private static final String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    private static final String BIG_QUERY_DATASET_ID =
            "table_row_to_storage_api_proto_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
    private static final String TABLE_NAME = "table_row_to_storage_api";
    private static final String FULL_TABLE_NAME = project + ":" + BIG_QUERY_DATASET_ID + "." + TABLE_NAME;

    private static final TableSchema BASE_TABLE_SCHEMA =
            new TableSchema()
                    .setFields(
                            ImmutableList.<TableFieldSchema>builder()
                                    .add(new TableFieldSchema().setType("STRING").setName("stringValue"))
                                    .add(new TableFieldSchema().setType("BYTES").setName("bytesValue"))
                                    .add(new TableFieldSchema().setType("INT64").setName("int64Value"))
                                    .add(new TableFieldSchema().setType("INTEGER").setName("intValue"))
                                    .add(new TableFieldSchema().setType("FLOAT64").setName("float64Value"))
                                    .add(new TableFieldSchema().setType("FLOAT").setName("floatValue"))
                                    .add(new TableFieldSchema().setType("BOOL").setName("boolValue"))
                                    .add(new TableFieldSchema().setType("BOOLEAN").setName("booleanValue"))
                                    .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValue"))
                                    .add(new TableFieldSchema().setType("TIME").setName("timeValue"))
                                    .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValue"))
                                    .add(new TableFieldSchema().setType("DATE").setName("dateValue"))
                                    .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue"))
                                    .add(
                                            new TableFieldSchema()
                                                    .setType("STRING")
                                                    .setMode("REPEATED")
                                                    .setName("arrayValue"))
                                    .build());

    private static final TableRow BASE_TABLE_ROW =
            new TableRow()
                    .set("stringValue", "string")
                    .set(
                            "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
                    .set("int64Value", "42")
                    .set("intValue", "43")
                    .set("float64Value", "2.8168")
                    .set("floatValue", "2.817")
                    .set("boolValue", "true")
                    .set("booleanValue", "true")
                    .set("timestampValue", "43")
                    .set("timeValue", "00:52:07.123456")
                    .set("datetimeValue", "2019-08-16T00:52:07.123456")
                    .set("dateValue", "2019-08-16")
                    .set("numericValue", "23.4")
                    .set("arrayValue", ImmutableList.of("hello", "goodbye"));

    private static final TableRow BASE_TABLE_ROW_READ_BACK =
            new TableRow()
                    .set("stringValue", "string")
                    .set(
                            "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
                    .set("int64Value", "42")
                    .set("intValue", "43")
                    .set("float64Value", 2.8168)
                    .set("floatValue", 2.817)
                    .set("boolValue", true)
                    .set("booleanValue", true)
                    .set("timestampValue", "43")
                    .set("timeValue", "00:52:07.123456")
                    .set("datetimeValue", "2019-08-16T00:52:07.123456")
                    .set("dateValue", "2019-08-16")
                    .set("numericValue", "23.4")
                    .set("arrayValue", ImmutableList.of("hello", "goodbye"));

    @BeforeClass
    public static void setupTestEnvironment() throws Exception {

        // Create one BQ dataset for all test cases.
        BQ_CLIENT.createNewDataset(project, BIG_QUERY_DATASET_ID);

        // Create table and insert data for new type query test cases.
        BQ_CLIENT.createNewTable(
                project,
                BIG_QUERY_DATASET_ID,
                new Table()
                        .setSchema(BASE_TABLE_SCHEMA)
                        .setTableReference(
                                new TableReference()
                                        .setTableId(TABLE_NAME)
                                        .setDatasetId(BIG_QUERY_DATASET_ID)
                                        .setProjectId(project)
                        )
        );
    }

    @AfterClass
    public static void cleanup() {
        LOG.info("Start to clean up tables and datasets.");
        BQ_CLIENT.deleteDataset(project, BIG_QUERY_DATASET_ID);
    }

    @Test
    public void testTableRowToStorageApiProtoIT() throws IOException, InterruptedException {
        runPipeline(Collections.singleton(BASE_TABLE_ROW));

        List<TableRow> actualTableRows = BQ_CLIENT
                .queryUnflattened(String.format("SELECT * FROM [%s]", FULL_TABLE_NAME), project, true);

        assertEquals(1, actualTableRows.size());
        assertEquals(BASE_TABLE_ROW_READ_BACK, actualTableRows.get(0));
    }

    private static void runPipeline(Iterable<TableRow> tableRows) {
        Pipeline p = Pipeline.create();
        p
                .apply("Create test cases", Create.of(tableRows))
                .apply("Write using Storage Write API",
                        BigQueryIO.<TableRow>write()
                                .to(FULL_TABLE_NAME)
                                .withFormatFunction(SerializableFunctions.identity())
                                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                );
        p.run().waitUntilFinish();
    }
}
