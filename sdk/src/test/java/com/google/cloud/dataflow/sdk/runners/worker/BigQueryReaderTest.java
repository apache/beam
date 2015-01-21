/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.mockito.Matchers.endsWith;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;

/**
 * Tests for BigQueryReader.
 *
 * <p>The tests make sure a simple scenario (reading two rows) work for the various kinds of fields
 * and modes.
 */
@RunWith(JUnit4.class)
public class BigQueryReaderTest {

  private static final String PROJECT_ID = "project";
  private static final String DATASET = "dataset";
  private static final String TABLE = "table";

  private static final String GET_TABLE_REQUEST_PATH =
      String.format("projects/%s/datasets/%s/tables/%s", PROJECT_ID, DATASET, TABLE);

  // This is a real response (with some unused fields removed) for the table created from this
  // schema:
  // [
  //  {"name":"name","type":"STRING"},
  //  {"name":"integer", "type":"INTEGER"},
  //  {"name":"float", "type":"FLOAT"},
  //  {"name":"bool", "type":"BOOLEAN"},
  //  {"name":"record", "type":"RECORD", "fields":[
  //    {"name": "nestedInt","type":"INTEGER"},
  //    {"name": "nestedFloat","type":"FLOAT"}
  //  ]},
  //  {"name":"repeatedInt", "type":"INTEGER", "mode":"REPEATED"},
  //  {"name":"repeatedFloat", "type":"FLOAT", "mode":"REPEATED"},
  //
  //  {"name":"repeatedRecord", "type":"RECORD", "mode":"REPEATED", "fields":[
  //    {"name": "bool", "type": "BOOLEAN"},
  //    {"name": "string", "type": "STRING"}
  //  ]}
  //]
  private static final String GET_TABLE_RESPONSE_JSON = "{\n"
      + " \"schema\": {\n"
      + "  \"fields\": [\n"
      + "   {\n"
      + "    \"name\": \"name\",\n"
      + "    \"type\": \"STRING\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"integer\",\n"
      + "    \"type\": \"INTEGER\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"float\",\n"
      + "    \"type\": \"FLOAT\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"bool\",\n"
      + "    \"type\": \"BOOLEAN\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"record\",\n"
      + "    \"type\": \"RECORD\",\n"
      + "    \"fields\": [\n"
      + "     {\n"
      + "      \"name\": \"nestedInt\",\n"
      + "      \"type\": \"INTEGER\"\n"
      + "     },\n"
      + "     {\n"
      + "      \"name\": \"nestedFloat\",\n"
      + "      \"type\": \"FLOAT\"\n"
      + "     }\n"
      + "    ]\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"repeatedInt\",\n"
      + "    \"type\": \"INTEGER\",\n"
      + "    \"mode\": \"REPEATED\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"repeatedFloat\",\n"
      + "    \"type\": \"FLOAT\",\n"
      + "    \"mode\": \"REPEATED\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"repeatedRecord\",\n"
      + "    \"type\": \"RECORD\",\n"
      + "    \"mode\": \"REPEATED\",\n"
      + "    \"fields\": [\n"
      + "     {\n"
      + "      \"name\": \"bool\",\n"
      + "      \"type\": \"BOOLEAN\"\n"
      + "     },\n"
      + "     {\n"
      + "      \"name\": \"string\",\n"
      + "      \"type\": \"STRING\"\n"
      + "     }\n"
      + "    ]\n"
      + "   }\n"
      + "  ]\n"
      + " },\n"
      + " \"numRows\": \"2\",\n"
      + " \"type\": \"TABLE\"\n"
      + "}";

  private static final String LIST_TABLE_DATA_REQUEST_PATH =
      String.format("projects/%s/datasets/%s/tables/%s/data", PROJECT_ID, DATASET, TABLE);

  // This is a real response (with some unused fields removed) for the table listed above, populated
  // with the following data:
  //{"name": "Arthur", "integer": 42, "float": 3.14159, "bool": "false",
  // "record": {"nestedInt": 43, "nestedFloat": "4.14159"},
  // "repeatedInt":[42, 43, 79]},
  //
  //{"name": "Allison", "integer": 79, "float": 2.71828, "bool": "true",
  // "record": {"nestedInt": 80, "nestedFloat": "3.71828"},
  // "repeatedFloat":[3.14159, 2.71828],
  // "repeatedRecord":[{"bool":"true","string":"hello"},
  //                   {"bool":"false","string":"world"}]}
  private static final String LIST_TABLEDATA_RESPONSE_JSON = "{\n"
      + " \"totalRows\": \"2\",\n"
      + " \"rows\": [\n"
      + "  {\n"
      + "   \"f\": [\n"
      + "    {\n"
      + "     \"v\": \"Arthur\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"42\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"3.14159\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"false\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": {\n"
      + "      \"f\": [\n"
      + "       {\n"
      + "        \"v\": \"43\"\n"
      + "       },\n"
      + "       {\n"
      + "        \"v\": \"4.14159\"\n"
      + "       }\n"
      + "      ]\n"
      + "     }\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "      {\n"
      + "       \"v\": \"42\"\n"
      + "      },\n"
      + "      {\n"
      + "       \"v\": \"43\"\n"
      + "      },\n"
      + "      {\n"
      + "       \"v\": \"79\"\n"
      + "      }\n"
      + "     ]\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "     ]\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "     ]\n"
      + "    }\n"
      + "   ]\n"
      + "  },\n"
      + "  {\n"
      + "   \"f\": [\n"
      + "    {\n"
      + "     \"v\": \"Allison\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"79\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"2.71828\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"true\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": {\n"
      + "      \"f\": [\n"
      + "       {\n"
      + "        \"v\": \"80\"\n"
      + "       },\n"
      + "       {\n"
      + "        \"v\": \"3.71828\"\n"
      + "       }\n"
      + "      ]\n"
      + "     }\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "     ]\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "      {\n"
      + "       \"v\": \"3.14159\"\n"
      + "      },\n"
      + "      {\n"
      + "       \"v\": \"2.71828\"\n"
      + "      }\n"
      + "     ]\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": [\n"
      + "      {\n"
      + "       \"v\": {\n"
      + "        \"f\": [\n"
      + "         {\n"
      + "          \"v\": \"true\"\n"
      + "         },\n"
      + "         {\n"
      + "          \"v\": \"hello\"\n"
      + "         }\n"
      + "        ]\n"
      + "       }\n"
      + "      },\n"
      + "      {\n"
      + "       \"v\": {\n"
      + "        \"f\": [\n"
      + "         {\n"
      + "          \"v\": \"false\"\n"
      + "         },\n"
      + "         {\n"
      + "          \"v\": \"world\"\n"
      + "         }\n"
      + "        ]\n"
      + "       }\n"
      + "      }\n"
      + "     ]\n"
      + "    }\n"
      + "   ]\n"
      + "  }\n"
      + " ]\n"
      + "}";

  @Mock
  private MockHttpTransport mockTransport;

  private Bigquery bigQueryClient;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockTransport.buildRequest(eq("GET"), endsWith(GET_TABLE_REQUEST_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(GET_TABLE_RESPONSE_JSON);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });
    when(mockTransport.buildRequest(eq("GET"), endsWith(LIST_TABLE_DATA_REQUEST_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(LIST_TABLEDATA_RESPONSE_JSON);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });
    when(mockTransport.supportsMethod("GET")).thenReturn(true);

    bigQueryClient = new Bigquery(mockTransport, Transport.getJsonFactory(), null);
  }

  @After
  public void tearDown() throws IOException {
    verify(mockTransport, atLeastOnce()).supportsMethod("GET");
    verifyNoMoreInteractions(mockTransport);
  }

  private void verifyTableGet() throws IOException {
    verify(mockTransport).buildRequest(eq("GET"), endsWith(GET_TABLE_REQUEST_PATH));
  }

  private void verifyTabledataList() throws IOException {
    verify(mockTransport).buildRequest(eq("GET"), endsWith(LIST_TABLE_DATA_REQUEST_PATH));
  }

  @Test
  public void testRead() throws Exception {
    BigQueryReader reader = new BigQueryReader(
        bigQueryClient,
        new TableReference().setProjectId(PROJECT_ID).setDatasetId(DATASET).setTableId(TABLE));

    Reader.ReaderIterator<TableRow> iterator = reader.iterator();
    Assert.assertTrue(iterator.hasNext());

    TableRow row = iterator.next();

    Assert.assertEquals("Arthur", row.get("name"));
    Assert.assertEquals("42", row.get("integer"));
    Assert.assertEquals(3.14159, row.get("float"));
    Assert.assertEquals(false, row.get("bool"));

    TableRow nested = (TableRow) row.get("record");
    Assert.assertEquals("43", nested.get("nestedInt"));
    Assert.assertEquals(4.14159, nested.get("nestedFloat"));

    Assert.assertEquals(Lists.newArrayList("42", "43", "79"), row.get("repeatedInt"));
    Assert.assertTrue(((List<?>) row.get("repeatedFloat")).isEmpty());
    Assert.assertTrue(((List<?>) row.get("repeatedRecord")).isEmpty());

    row = iterator.next();

    Assert.assertEquals("Allison", row.get("name"));
    Assert.assertEquals("79", row.get("integer"));
    Assert.assertEquals(2.71828, row.get("float"));
    Assert.assertEquals(true, row.get("bool"));

    nested = (TableRow) row.get("record");
    Assert.assertEquals("80", nested.get("nestedInt"));
    Assert.assertEquals(3.71828, nested.get("nestedFloat"));

    Assert.assertTrue(((List<?>) row.get("repeatedInt")).isEmpty());
    Assert.assertEquals(Lists.newArrayList(3.14159, 2.71828), row.get("repeatedFloat"));

    @SuppressWarnings("unchecked")
    List<TableRow> nestedRecords = (List<TableRow>) row.get("repeatedRecord");
    Assert.assertEquals(2, nestedRecords.size());
    Assert.assertEquals("hello", nestedRecords.get(0).get("string"));
    Assert.assertEquals(true, nestedRecords.get(0).get("bool"));
    Assert.assertEquals("world", nestedRecords.get(1).get("string"));
    Assert.assertEquals(false, nestedRecords.get(1).get("bool"));

    Assert.assertFalse(iterator.hasNext());

    verifyTableGet();
    verifyTabledataList();
  }
}
