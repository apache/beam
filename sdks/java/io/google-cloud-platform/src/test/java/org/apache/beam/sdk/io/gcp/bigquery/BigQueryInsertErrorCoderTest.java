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

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link BigQueryInsertErrorCoder}. */
@RunWith(JUnit4.class)
public class BigQueryInsertErrorCoderTest {

  private static final Coder<BigQueryInsertError> TEST_CODER = BigQueryInsertErrorCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    BigQueryInsertError value =
        new BigQueryInsertError(
            new TableRow().setF(Collections.singletonList(new TableCell().setV("Value"))),
            new TableDataInsertAllResponse.InsertErrors()
                .setIndex(0L)
                .setErrors(
                    Collections.singletonList(
                        new ErrorProto()
                            .setReason("a Reason")
                            .setLocation("A location")
                            .setMessage("A message")
                            .setDebugInfo("The debug info"))),
            new TableReference()
                .setProjectId("dummy-project-id")
                .setDatasetId("dummy-dataset-id")
                .setTableId("dummy-table-id"));

    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
  }
}
