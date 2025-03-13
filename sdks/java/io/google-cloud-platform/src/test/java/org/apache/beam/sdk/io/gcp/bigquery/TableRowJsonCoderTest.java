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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link TableRowJsonCoder}. */
@RunWith(JUnit4.class)
public class TableRowJsonCoderTest {

  private static class TableRowBuilder {
    private TableRow row;

    public TableRowBuilder() {
      row = new TableRow();
    }

    public TableRowBuilder set(String fieldName, Object value) {
      row.set(fieldName, value);
      return this;
    }

    public TableRow build() {
      return row;
    }
  }

  private static final Coder<TableRow> TEST_CODER = TableRowJsonCoder.of();

  private static final List<TableRow> TEST_VALUES =
      Arrays.asList(
          new TableRowBuilder().build(),
          new TableRowBuilder().set("a", "1").build(),
          new TableRowBuilder().set("b", 3.14).build(),
          new TableRowBuilder().set("a", "1").set("b", true).set("c", "hi").build());

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (TableRow value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testLargeRow() throws Exception {
    String val = StringUtils.repeat("BEAM", 10 * 1024 * 1024); // 40 MB
    TableRow testValue = new TableRowBuilder().set("a", val).set("b", "1").build();
    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, testValue);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList(
          "e30", "eyJhIjoiMSJ9", "eyJiIjozLjE0fQ", "eyJhIjoiMSIsImIiOnRydWUsImMiOiJoaSJ9");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(TableRow.class)));
  }
}
