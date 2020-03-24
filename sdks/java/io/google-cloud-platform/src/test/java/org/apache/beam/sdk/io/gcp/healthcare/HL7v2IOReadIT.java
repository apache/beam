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
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.MESSAGES;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.NUM_ADT;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.deleteAllHL7v2Messages;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.writeHL7v2Messages;

import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOReadIT {
  private HL7v2IOTestOptions options;
  private transient HealthcareApiClient client;

  @Before
  public void setup() throws Exception {
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    PipelineOptionsFactory.register(HL7v2IOTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HL7v2IOTestOptions.class);
    options.setHL7v2Store(
        "projects/jferriero-dev/locations/us-central1/datasets/raw-dataset/hl7V2Stores/jake-hl7");
    // Create HL7 messages and write them to HL7v2 Store.
    writeHL7v2Messages(this.client, options.getHL7v2Store());
  }

  @After
  public void tearDown() throws Exception {
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    deleteAllHL7v2Messages(this.client, options.getHL7v2Store());
  }

  @Test
  public void testHL7v2IORead() throws Exception {
    // Should read all messages.
    Pipeline pipeline = Pipeline.create(options);
    HL7v2IO.Read.Result result =
        pipeline
            .apply(
                new HL7v2IO.ListHL7v2MessageIDs(Collections.singletonList(options.getHL7v2Store())))
            .apply(HL7v2IO.readAll());
    PCollection<Long> numReadMessages =
        result.getMessages().setCoder(new HL7v2MessageCoder()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo((long) MESSAGES.size());
    PAssert.that(result.getFailedReads()).empty();

    pipeline.run();
  }

  @Test
  public void testHL7v2IOFilteredRead() throws Exception {
    final String filter = "messageType = \"ADT\"";
    // Should read only messages matching the filter.
    Pipeline pipeline = Pipeline.create(options);
    HL7v2IO.Read.Result result =
        pipeline
            .apply(
                new HL7v2IO.ListHL7v2MessageIDs(
                    Collections.singletonList(options.getHL7v2Store()), filter))
            .apply(HL7v2IO.readAll());
    PCollection<Long> numReadMessages =
        result.getMessages().setCoder(new HL7v2MessageCoder()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo(NUM_ADT);
    PAssert.that(result.getFailedReads()).empty();

    pipeline.run();
  }
}
