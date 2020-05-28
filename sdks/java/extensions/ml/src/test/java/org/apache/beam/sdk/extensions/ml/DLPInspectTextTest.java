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
package org.apache.beam.sdk.extensions.ml;

import static org.junit.Assert.assertThrows;

import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DLPInspectTextTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private static final Integer BATCH_SIZE_SMALL = 200;
  private static final String DELIMITER = ";";
  private static final String TEMPLATE_NAME = "test_template";
  private static final String PROJECT_ID = "test_id";

  @Test
  public void throwsExceptionWhenDeidentifyConfigAndTemplatesAreEmpty() {
    assertThrows(
        "Either inspectTemplateName or inspectConfig must be supplied!",
        IllegalArgumentException.class,
        () ->
            DLPInspectText.newBuilder()
                .setProjectId(PROJECT_ID)
                .setBatchSizeBytes(BATCH_SIZE_SMALL)
                .setColumnDelimiter(DELIMITER)
                .build());
  }

  @Test
  public void throwsExceptionWhenDelimiterIsNullAndHeadersAreSet() {
    PCollectionView<List<String>> header =
        testPipeline.apply(Create.of("header")).apply(View.asList());
    assertThrows(
        "Column delimiter should be set if headers are present.",
        IllegalArgumentException.class,
        () ->
            DLPInspectText.newBuilder()
                .setProjectId(PROJECT_ID)
                .setBatchSizeBytes(BATCH_SIZE_SMALL)
                .setInspectTemplateName(TEMPLATE_NAME)
                .setHeaderColumns(header)
                .build());
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void throwsExceptionWhenBatchSizeIsTooLarge() {
    assertThrows(
        String.format(
            "Batch size is too large! It should be smaller or equal than %d.",
            DLPInspectText.DLP_PAYLOAD_LIMIT_BYTES),
        IllegalArgumentException.class,
        () ->
            DLPInspectText.newBuilder()
                .setProjectId(PROJECT_ID)
                .setBatchSizeBytes(Integer.MAX_VALUE)
                .setInspectTemplateName(TEMPLATE_NAME)
                .setColumnDelimiter(DELIMITER)
                .build());
  }

  @Test
  public void throwsExceptionWhenDelimiterIsSetAndHeadersAreNot() {
    assertThrows(
        "Column headers should be supplied when delimiter is present.",
        IllegalArgumentException.class,
        () ->
            DLPInspectText.newBuilder()
                .setProjectId(PROJECT_ID)
                .setBatchSizeBytes(BATCH_SIZE_SMALL)
                .setInspectTemplateName(TEMPLATE_NAME)
                .setColumnDelimiter(DELIMITER)
                .build());
  }
}
