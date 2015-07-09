/*
 * Copyright (C) 2015 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
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
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.collect.Lists;

import org.junit.Assert;
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
  private static final String QUERY_JOB_ID = "query_job_id";
  private static final String QUERY_JOB_RESPONSE_DATASET = "query_job_response_dataset";
  private static final String QUERY_JOB_RESPONSE_TEMP_TABLE = "query_job_response_temp_table";

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
  // ]
  private static final String GET_TABLE_RESPONSE_JSON =
      "{\n"
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
  // {"name": "Arthur", "integer": 42, "float": 3.14159, "bool": "false",
  // "record": {"nestedInt": 43, "nestedFloat": "4.14159"},
  // "repeatedInt":[42, 43, 79]},
  //
  // {"name": "Allison", "integer": 79, "float": 2.71828, "bool": "true",
  // "record": {"nestedInt": 80, "nestedFloat": "3.71828"},
  // "repeatedFloat":[3.14159, 2.71828],
  // "repeatedRecord":[{"bool":"true","string":"hello"},
  //                   {"bool":"false","string":"world"}]}
  private static final String LIST_TABLEDATA_RESPONSE_JSON =
      "{\n"
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


  private static final String INSERT_QUERY_JOB_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/jobs", PROJECT_ID);

  // This is the actual response received for following requests (parameterized identifiers were
  // updated).
  //  POST https://www.googleapis.com/bigquery/v2/projects/project/jobs
  //
  //  {
  //   "configuration": {
  //    "query": {
  //     "query": "SELECT name, integer from dataset.table"
  //    }
  //   }
  //  }
  private static final String INSERT_QUERY_JOB_RESPONSE = String.format(
      "{\n"
      + "\n"
      + " \"kind\": \"bigquery#job\",\n"
      + " \"etag\": \"\\\"Gn3Hpo5WaKnpFuT457VBDNMgZBw/GF2DiLTiF0s2MwsdQlV4UB-xaew\\\"\",\n"
      + " \"id\": \"%1$s:%2$s\",\n"
      + " \"selfLink\": \"https://www.googleapis.com/bigquery/v2/projects/%1$s/jobs/%2$s\",\n"
      + " \"jobReference\": {\n"
      + "  \"projectId\": \"%1$s\",\n"
      + "  \"jobId\": \"%2$s\"\n"
      + " },\n"
      + " \"configuration\": {\n"
      + "  \"query\": {\n"
      + "   \"query\": \"SELECT name, integer from %4$s.%5$s\",\n"
      + "   \"destinationTable\": {\n"
      + "    \"projectId\": \"%1$s\",\n"
      + "    \"datasetId\": \"%4$s\",\n"
      + "    \"tableId\": \"%3$s\"\n"
      + "   },\n"
      + "   \"createDisposition\": \"CREATE_IF_NEEDED\",\n"
      + "   \"writeDisposition\": \"WRITE_TRUNCATE\"\n"
      + "  }\n"
      + " },\n"
      + " \"status\": {\n"
      + "  \"state\": \"RUNNING\"\n"
      + " },\n"
      + " \"statistics\": {\n"
      + "  \"creationTime\": \"1433378500260\",\n"
      + "  \"startTime\": \"1433378500833\"\n"
      + " },\n"
      + " \"user_email\": \"user@gmail.com\"\n"
      + "}",
      PROJECT_ID, QUERY_JOB_ID, QUERY_JOB_RESPONSE_TEMP_TABLE, QUERY_JOB_RESPONSE_DATASET, TABLE);


  private static final String GET_QUERY_JOB_STATUS_REQUEST_PATH = String.format(
      "https://www.googleapis.com/bigquery/v2/projects/%s/jobs/%s", PROJECT_ID, QUERY_JOB_ID);


  // This is the actual response received for following requests (parameterized identifiers were
  // updated).
  // GET
  // https://www.googleapis.com/bigquery/v2/projects/project/jobs/query_job_id
  private static final String GET_QUERY_JOB_STATUS_RESPONSE_JSON = String.format(
      "{\n"
      + "\n"
      + " \"kind\": \"bigquery#job\",\n"
      + " \"etag\": \"\\\"Gn3Hpo5WaKnpFuT457VBDNMgZBw/ZVEFinsDS6AZ04jebPWC9_isCpY\\\"\",\n"
      + " \"id\": \"%1$s:%4$s\",\n"
      + " \"selfLink\": \"https://www.googleapis.com/bigquery/v2/projects/%1$s/jobs/%4$s\",\n"
      + " \"jobReference\": {\n"
      + "  \"projectId\": \"%1$s\",\n"
      + "  \"jobId\": \"%4$s\"\n"
      + " },\n"
      + " \"configuration\": {\n"
      + "  \"query\": {\n"
      + "   \"query\": \"SELECT name, integer from test_chamikara.bigquery_reader_test\",\n"
      + "   \"destinationTable\": {\n"
      + "    \"projectId\": \"%1$s\",\n"
      + "    \"datasetId\": \"%2$s\",\n"
      + "    \"tableId\": \"%3$s\"\n"
      + "   },\n"
      + "   \"createDisposition\": \"CREATE_IF_NEEDED\",\n"
      + "   \"writeDisposition\": \"WRITE_TRUNCATE\",\n"
      + "   \"priority\": \"INTERACTIVE\",\n"
      + "   \"useQueryCache\": true\n"
      + "  }\n"
      + " },\n"
      + " \"status\": {\n"
      + "  \"state\": \"DONE\"\n"
      + " },\n"
      + " \"statistics\": {\n"
      + "  \"creationTime\": \"1433374960768\",\n"
      + "  \"startTime\": \"1433374961242\",\n"
      + "  \"endTime\": \"1433374961532\",\n"
      + "  \"totalBytesProcessed\": \"33\",\n"
      + "  \"query\": {\n"
      + "   \"totalBytesProcessed\": \"33\",\n"
      + "   \"cacheHit\": false\n"
      + "  }\n"
      + " },\n"
      + " \"user_email\": \"user@gmail.com\"\n"
      + "}",
      PROJECT_ID, QUERY_JOB_RESPONSE_DATASET, QUERY_JOB_RESPONSE_TEMP_TABLE, QUERY_JOB_ID);

  private static final String LIST_QUERY_TABLE_DATA_REQUEST_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s/data",
          PROJECT_ID, QUERY_JOB_RESPONSE_DATASET, QUERY_JOB_RESPONSE_TEMP_TABLE);

  // This is the actual response received for following requests (parameterized identifiers were
  // updated).
  // GET
  // https://www.googleapis.com/bigquery/v2/projects/project/datasets/query_job_response_dataset/
  //   tables/query_job_response_temp_table/data
  private static final String LIST_QUERY_TABLE_DATA_RESPONSE =
      "{\n"
      + " \"kind\": \"bigquery#tableDataList\",\n"
      + " \"etag\": \"\\\"Gn3Hpo5WaKnpFuT457VBDNMgZBw/6Y6hxVy6yTmtI2EEgHfqg3w49yU\\\"\",\n"
      + " \"totalRows\": \"2\",\n"
      + " \"rows\": [\n"
      + "  {\n"
      + "   \"f\": [\n"
      + "    {\n"
      + "     \"v\": \"Arthur\"\n"
      + "    },\n"
      + "    {\n"
      + "     \"v\": \"42\"\n"
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
      + "    }\n"
      + "   ]\n"
      + "  }\n"
      + " ]\n"
      + "}";

  private static final String QUERY_TABLE_GET_REQUEST_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s",
          PROJECT_ID, QUERY_JOB_RESPONSE_DATASET, QUERY_JOB_RESPONSE_TEMP_TABLE);

  // This is the actual response received for following requests (parameterized identifiers were
  // updated).
  // GET
  // https://www.googleapis.com/bigquery/v2/projects/project/datasets/query_job_response_dataset/
  //   tables/query_job_response_temp_table
  private static final String QUERY_TABLE_GET_RESPONSE = String.format(
      "{\n"
      + "\n"
      + " \"kind\": \"bigquery#table\",\n"
      + " \"etag\": \"\\\"Gn3Hpo5WaKnpFuT457VBDNMgZBw/MTQzMzM3ODUwMDg4NA\\\"\",\n"
      + " \"id\": \"%1$s:%2$s.%3$s\",\n"
      + " \"selfLink\": \"https://www.googleapis.com/bigquery/v2/projects/%1$s/datasets/%2$s/"
      + "tables/%3$s\",\n"
      + " \"tableReference\": {\n"
      + "  \"projectId\": \"%1$s\",\n"
      + "  \"datasetId\": \"%2$s\",\n"
      + "  \"tableId\": \"%3$s\"\n"
      + " },\n"
      + " \"schema\": {\n"
      + "  \"fields\": [\n"
      + "   {\n"
      + "    \"name\": \"name\",\n"
      + "    \"type\": \"STRING\",\n"
      + "    \"mode\": \"NULLABLE\"\n"
      + "   },\n"
      + "   {\n"
      + "    \"name\": \"integer\",\n"
      + "    \"type\": \"INTEGER\",\n"
      + "    \"mode\": \"NULLABLE\"\n"
      + "   }\n"
      + "  ]\n"
      + " },\n"
      + " \"numBytes\": \"33\",\n"
      + " \"numRows\": \"2\",\n"
      + " \"creationTime\": \"1433374961476\",\n"
      + " \"expirationTime\": \"1433464900889\",\n"
      + " \"lastModifiedTime\": \"1433378500884\",\n"
      + " \"type\": \"TABLE\"\n"
      + "}",
      PROJECT_ID, QUERY_JOB_RESPONSE_DATASET, QUERY_JOB_RESPONSE_TEMP_TABLE);

  private static final String QUERY_DATASET_INSERT_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/datasets", PROJECT_ID);

  private static final String QUERY_DATASET_INSERT_RESPONSE = String.format(
      "{\n"
      + "\n"
      + " \"kind\": \"bigquery#dataset\",\n"
      + " \"etag\": \"\\\"Gn3Hpo5WaKnpFuT457VBDNMgZBw/67ZQLsO6a6iDLJ71ReSC-LWBBw4\\\"\",\n"
      + " \"id\": \"%1$s:%2$s\",\n"
      + " \"selfLink\": \"https://www.googleapis.com/bigquery/v2/projects/%1$s/datasets/%2$s\",\n"
      + " \"datasetReference\": {\n"
      + "  \"datasetId\": \"%2$s\",\n"
      + "  \"projectId\": \"%1$s\"\n"
      + " },\n"
      + " \"access\": [\n"
      + "  {\n"
      + "   \"role\": \"OWNER\",\n"
      + "   \"specialGroup\": \"projectOwners\"\n"
      + "  },\n"
      + "  {\n"
      + "   \"role\": \"WRITER\",\n"
      + "   \"specialGroup\": \"projectWriters\"\n"
      + "  },\n"
      + "  {\n"
      + "   \"role\": \"READER\",\n"
      + "   \"specialGroup\": \"projectReaders\"\n"
      + "  }\n"
      + " ],\n"
      + " \"creationTime\": \"1436219427054\",\n"
      + " \"lastModifiedTime\": \"1436219427054\"\n"
      + "}",
      PROJECT_ID, QUERY_JOB_RESPONSE_DATASET);

  private static final String QUERY_TABLE_DELETE_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/", PROJECT_ID);

  private static final String QUERY_TABLE_DELETE_RESPONSE = ""; // Empty body

  private static final String QUERY_DATASET_DELETE_PATH =
      String.format("https://www.googleapis.com/bigquery/v2/projects/%s/", PROJECT_ID);

  private static final String QUERY_DATASET_DELETE_RESPONSE = ""; // Empty body

  private static final String QUERY = "SELECT name, integer from dataset.table";

  @Mock
  private MockHttpTransport mockTransport;

  private Bigquery bigQueryClient;

  private void verifyDatasetInsert() throws IOException {
    verify(mockTransport, times(1)).buildRequest(eq("POST"), endsWith(QUERY_DATASET_INSERT_PATH));
  }

  private void verifyInsertQueryJob() throws IOException {
    verify(mockTransport, times(1)).buildRequest(eq("POST"), endsWith(INSERT_QUERY_JOB_PATH));
  }

  private void verifyQueryJobStatus() throws IOException {
    verify(mockTransport, times(1))
        .buildRequest(eq("GET"), endsWith(GET_QUERY_JOB_STATUS_REQUEST_PATH));
  }

  private void verifyQueryTableData() throws IOException {
    verify(mockTransport, times(1))
        .buildRequest(eq("GET"), endsWith(LIST_QUERY_TABLE_DATA_REQUEST_PATH));
  }

  private void verifyQueryTableGet() throws IOException {
    verify(mockTransport, times(1)).buildRequest(eq("GET"), endsWith(QUERY_TABLE_GET_REQUEST_PATH));
  }

  private void verifyTableDelete() throws IOException {
    verify(mockTransport, times(2)).buildRequest(eq("DELETE"), contains(QUERY_TABLE_DELETE_PATH));
  }

  private void verifyDatasetDelete() throws IOException {
    verify(mockTransport, times(2)).buildRequest(eq("DELETE"), contains(QUERY_DATASET_DELETE_PATH));
  }

  @Test
  public void testReadQuery() throws Exception {
    setUpMockQuery();

    bigQueryClient = new Bigquery(mockTransport, Transport.getJsonFactory(), null);
    BigQueryReader reader = new BigQueryReader(bigQueryClient, QUERY, PROJECT_ID);
    Reader.ReaderIterator<WindowedValue<TableRow>> iterator = reader.iterator();

    assertTrue(iterator.hasNext());
    TableRow row = iterator.next().getValue();

    assertEquals("Arthur", row.get("name"));
    assertEquals("42", row.get("integer"));

    row = iterator.next().getValue();
    assertEquals("Allison", row.get("name"));
    assertEquals("79", row.get("integer"));

    iterator.close();

    verify(mockTransport, atLeastOnce()).supportsMethod("GET");
    verify(mockTransport, atLeastOnce()).supportsMethod("DELETE");

    verifyDatasetInsert();
    verifyInsertQueryJob();
    verifyQueryJobStatus();
    verifyQueryTableData();
    verifyQueryTableGet();
    verifyTableDelete();
    verifyDatasetDelete();

    verifyNoMoreInteractions(mockTransport);
  }

  void setUpMockQuery() throws IOException {
    MockitoAnnotations.initMocks(this);

    when(mockTransport.buildRequest(eq("POST"), endsWith(QUERY_DATASET_INSERT_PATH)))
    .thenAnswer(new Answer<LowLevelHttpRequest>() {
      @Override
      public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
        MockLowLevelHttpResponse response =
            new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(QUERY_DATASET_INSERT_RESPONSE);
        return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
            .setResponse(response);
      }
    });

    when(mockTransport.buildRequest(eq("POST"), endsWith(INSERT_QUERY_JOB_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(INSERT_QUERY_JOB_RESPONSE);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });

    when(mockTransport.buildRequest(eq("GET"), endsWith(GET_QUERY_JOB_STATUS_REQUEST_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(GET_QUERY_JOB_STATUS_RESPONSE_JSON);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });

    when(mockTransport.buildRequest(eq("GET"), endsWith(LIST_QUERY_TABLE_DATA_REQUEST_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(LIST_QUERY_TABLE_DATA_RESPONSE);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });

    when(mockTransport.buildRequest(eq("GET"), endsWith(QUERY_TABLE_GET_REQUEST_PATH)))
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(QUERY_TABLE_GET_RESPONSE);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });

    when(mockTransport.buildRequest(eq("DELETE"), contains(QUERY_TABLE_DELETE_PATH)))
    .thenAnswer(new Answer<LowLevelHttpRequest>() {
      @Override
      public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
        MockLowLevelHttpResponse response =
            new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(QUERY_TABLE_DELETE_RESPONSE);
        return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
            .setResponse(response);
      }
    });

    when(mockTransport.buildRequest(eq("DELETE"), contains(QUERY_DATASET_DELETE_PATH)))
    .thenAnswer(new Answer<LowLevelHttpRequest>() {
      @Override
      public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
        MockLowLevelHttpResponse response =
            new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(QUERY_DATASET_DELETE_RESPONSE);
        return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
            .setResponse(response);
      }
    });

    when(mockTransport.supportsMethod("GET")).thenReturn(true);
    when(mockTransport.supportsMethod("DELETE")).thenReturn(true);
  }

  void setUpMockTable() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockTransport.buildRequest(eq("GET"), endsWith(GET_TABLE_REQUEST_PATH)))
        .thenThrow(new IOException())
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(GET_TABLE_RESPONSE_JSON);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });
    when(mockTransport.buildRequest(eq("GET"), endsWith(LIST_TABLE_DATA_REQUEST_PATH)))
        .thenThrow(new IOException())
        .thenAnswer(new Answer<LowLevelHttpRequest>() {
          @Override
          public LowLevelHttpRequest answer(InvocationOnMock invocation) throws Throwable {
            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(LIST_TABLEDATA_RESPONSE_JSON);
            return new MockLowLevelHttpRequest((String) invocation.getArguments()[1])
                .setResponse(response);
          }
        });
    when(mockTransport.supportsMethod("GET")).thenReturn(true);
  }

  private void verifyTableGet() throws IOException {
    verify(mockTransport, times(2)).buildRequest(eq("GET"), endsWith(GET_TABLE_REQUEST_PATH));
  }

  private void verifyTabledataList() throws IOException {
    verify(mockTransport, times(2)).buildRequest(eq("GET"), endsWith(LIST_TABLE_DATA_REQUEST_PATH));
  }

  @Test
  public void testReadTable() throws Exception {
    setUpMockTable();

    bigQueryClient = new Bigquery(mockTransport, Transport.getJsonFactory(), null);

    BigQueryReader reader = new BigQueryReader(
        bigQueryClient,
        new TableReference().setProjectId(PROJECT_ID).setDatasetId(DATASET).setTableId(TABLE));

    Reader.ReaderIterator<WindowedValue<TableRow>> iterator = reader.iterator();
    Assert.assertTrue(iterator.hasNext());

    TableRow row = iterator.next().getValue();

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

    row = iterator.next().getValue();

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

    verify(mockTransport, atLeastOnce()).supportsMethod("GET");
    verifyNoMoreInteractions(mockTransport);
  }
}
