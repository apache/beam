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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;

import java.io.Serializable;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for DataCatalog+GCS. */
@RunWith(JUnit4.class)
public class DataCatalogGCSIT implements Serializable {

  private static final Schema ID_NAME_TYPE_SCHEMA =
      Schema.builder()
          .addNullableField("id", INT32)
          .addNullableField("name", STRING)
          .addNullableField("type", STRING)
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReadFromGCS() throws Exception {
    String gcsEntryId =
        "`datacatalog`" // this is part of the resource name in DataCatalog, so it has to be
            + ".`entry`" // different from the table provider name ("dc" in this test)
            + ".`apache-beam-testing`"
            + ".`us-central1`"
            + ".`samples`"
            + ".`integ_test_small_csv_test_1`";

    try (DataCatalogTableProvider tableProvider =
        DataCatalogTableProvider.create(
            pipeline.getOptions().as(DataCatalogPipelineOptions.class))) {
      PCollection<Row> result =
          pipeline.apply(
              "query",
              SqlTransform.query("SELECT id, name, type FROM " + gcsEntryId)
                  .withDefaultTableProvider("dc", tableProvider));

      pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(true);
      PAssert.that(result)
          .containsInAnyOrder(
              row(1, "customer1", "test"),
              row(2, "customer2", "test"),
              row(3, "customer1", "test"),
              row(4, "customer2", "test"));
      pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
    }
  }

  private Row row(int id, String name, String type) {
    return Row.withSchema(ID_NAME_TYPE_SCHEMA).addValues(id, name, type).build();
  }
}
