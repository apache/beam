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

package org.apache.beam.sdk.io.gcp.bigquery.schematransform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQuerySchemaIOConfigurationTest {

  private static final String COMMON_QUERY = "select * from example";

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  static final List<ReadTestCase> testCases = Arrays.asList(
      ReadTestCase.of(
          BigQuerySchemaIOConfiguration.builderOfQueryType(""),
          InvalidConfigurationException.class
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput(),
          commonBaseQueryWant()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput()
              .setUseAvroLogicalTypes(true),
          commonBaseQueryWant().useAvroLogicalTypes()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput()
              .setUseAvroLogicalTypes(false),
          commonBaseQueryWant()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setFormat("ARROW"),
          commonBaseQueryWant().withFormat(DataFormat.ARROW)
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setFormat("BADFORMAT"),
          IllegalArgumentException.class
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setKmsKey("somekmskey"),
          commonBaseQueryWant().withKmsKey("somekmskey")
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setMethod("DEFAULT"),
          commonBaseQueryWant().withMethod(TypedRead.Method.DEFAULT)
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setMethod("BADMETHOD"),
          IllegalArgumentException.class
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setWithoutFlattenResults(true),
          commonBaseQueryWant().withoutResultFlattening()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setWithoutFlattenResults(false),
          commonBaseQueryWant()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setWithoutValidation(true),
          commonBaseQueryWant().withoutValidation()
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setQueryLocation("somelocation"),
          commonBaseQueryWant().withQueryLocation("somelocation")
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setQueryPriority("BATCH"),
          commonBaseQueryWant().withQueryPriority(QueryPriority.BATCH)
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setQueryPriority("BADVALUE"),
          IllegalArgumentException.class
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setQueryTempDataset("sometempdataset"),
          commonBaseQueryWant().withQueryTempDataset("sometempdataset")
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setRowRestriction("somerowrestriction"),
          commonBaseQueryWant().withRowRestriction("somerowrestriction")
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setSelectedFields(Arrays.asList("field1", "field2", "field3")),
          commonBaseQueryWant().withSelectedFields(Arrays.asList("field1", "field2", "field3"))
      ),
      ReadTestCase.of(
          commonQueryConfigurationInput().setWithTemplateCompatibility(true),
          commonBaseQueryWant().withTemplateCompatibility()
      )
  );

  static class ReadTestCase {

    BigQuerySchemaIOConfiguration.Builder input;
    BigQueryIO.TypedRead<TableRow> want;
    Class<? extends Exception> wantErr;

    ReadTestCase(BigQuerySchemaIOConfiguration.Builder input, BigQueryIO.TypedRead<TableRow> want,
        Class<? extends Exception> wantErr) {
      this.input = input;
      this.want = want;
      this.wantErr = wantErr;
    }

    static ReadTestCase of(BigQuerySchemaIOConfiguration.Builder input,
        BigQueryIO.TypedRead<TableRow> want) {
      return new ReadTestCase(input, want, null);
    }

    static ReadTestCase of(BigQuerySchemaIOConfiguration.Builder input,
        Class<? extends Exception> wantErr) {
      return new ReadTestCase(input, null, wantErr);
    }
  }

  @Test
  public void testToQueryTypedReadDefaults() {
    String query = "select * from example";
    BigQuerySchemaIOConfiguration input = BigQuerySchemaIOConfiguration.builderOfQueryType(query)
        .build();
    assertEquals(query, input.getQuery());
    assertTrue(input.getUseStandardSql());
  }

  @Test
  public void testToQueryTypedRead() {
    for (ReadTestCase caze : testCases) {
      if (caze.wantErr != null) {
        assertThrows(caze.wantErr, () -> caze.input.build().toQueryTypedRead());
        continue;
      }
      Map<Identifier, Item> wantDisplayData = DisplayData.from(caze.want).asMap();
      Map<Identifier, Item> gotDisplayData = DisplayData.from(caze.input.build().toQueryTypedRead())
          .asMap();
      Set<Identifier> keys = new HashSet<>();
      keys.addAll(wantDisplayData.keySet());
      keys.addAll(gotDisplayData.keySet());

      for (Identifier key : keys) {
        Item want = wantDisplayData.get(key);
        Item got = gotDisplayData.get(key);
        assertEquals(want, got);
      }
    }
  }
  
  static BigQuerySchemaIOConfiguration.Builder commonQueryConfigurationInput() {
    return BigQuerySchemaIOConfiguration.builderOfQueryType(COMMON_QUERY);
  }
  
  static BigQueryIO.TypedRead<TableRow> commonBaseQueryWant() {
    return BigQueryIO.readTableRowsWithSchema().fromQuery(COMMON_QUERY).usingStandardSql();
  }
}
