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
package org.apache.beam.sdk.testing;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Test;

/**
 * Validates the options parsing from json or not.
 */
public class TestPipelineHandlerTest {
    @Test
    public void parseOptionsWhenNotJson() {
        final String value = System.getProperty(
                TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);
        try {
            System.setProperty(TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
                    "[\"--jobName=test\",\"--tempLocation=OFF\"]");
            final PipelineOptions fromJson = TestPipelineHandler.testingPipelineOptions();
            assertEquals("test", fromJson.getJobName());
            assertEquals("OFF", fromJson.getTempLocation());

            System.setProperty(TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
                    "--jobName=test --tempLocation=OFF");
            final PipelineOptions fromCli = TestPipelineHandler.testingPipelineOptions();
            assertEquals("test", fromCli.getJobName());
            assertEquals("OFF", fromCli.getTempLocation());

            System.setProperty(TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
                    "    \n\n\n   --jobName=test\n    --tempLocation=OFF\n");
            final PipelineOptions fromWeirdCli = TestPipelineHandler.testingPipelineOptions();
            assertEquals("test", fromWeirdCli.getJobName());
            assertEquals("OFF", fromWeirdCli.getTempLocation());
        } finally {
            if (value == null) {
                System.clearProperty(TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);
            } else {
                System.setProperty(TestPipelineHandler.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS, value);
            }
        }
    }
}
