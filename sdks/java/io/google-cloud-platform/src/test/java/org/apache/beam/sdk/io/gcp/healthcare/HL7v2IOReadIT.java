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

import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.HEALTHCARE_DATASET_TEMPLATE;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.MESSAGES;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.NUM_ADT;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.deleteAllHL7v2Messages;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.writeHL7v2Messages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.security.SecureRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOReadIT {
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String HL7V2_STORE_NAME =
      "hl7v2_store_"
          + System.currentTimeMillis()
          + "_"
          + (new SecureRandom().nextInt(32))
          + "_read_it";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void createHL7v2tore() throws IOException {
    String project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.createHL7v2Store(healthcareDataset, HL7V2_STORE_NAME);
  }

  @AfterClass
  public static void deleteHL7v2tore() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteHL7v2Store(healthcareDataset + "/hl7V2Stores/" + HL7V2_STORE_NAME);
  }

  @Before
  public void setup() throws Exception {
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    // Create HL7 messages and write them to HL7v2 Store.
    writeHL7v2Messages(this.client, healthcareDataset + "/hl7V2Stores/" + HL7V2_STORE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    deleteAllHL7v2Messages(this.client, healthcareDataset + "/hl7V2Stores/" + HL7V2_STORE_NAME);
  }

  @Test
  public void testHL7v2IO_ListHL7v2Messages() throws Exception {
    // Should read all messages.
    Pipeline pipeline = Pipeline.create();
    PCollection<HL7v2Message> result =
        pipeline.apply(HL7v2IO.read(healthcareDataset + "/hl7V2Stores/" + HL7V2_STORE_NAME));
    PCollection<Long> numReadMessages =
        result.setCoder(HL7v2MessageCoder.of()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo((long) MESSAGES.size());

    PAssert.that(result)
        .satisfies(
            input -> {
              for (HL7v2Message elem : input) {
                assertFalse(elem.getName().isEmpty());
                assertFalse(elem.getData().isEmpty());
                assertFalse(elem.getMessageType().isEmpty());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testHL7v2IO_ListHL7v2Messages_filtered() throws Exception {
    final String adtFilter = "messageType = \"ADT\"";
    // Should read all messages.
    Pipeline pipeline = Pipeline.create();
    PCollection<HL7v2Message> result =
        pipeline.apply(
            HL7v2IO.readWithFilter(
                healthcareDataset + "/hl7V2Stores/" + HL7V2_STORE_NAME, adtFilter));
    PCollection<Long> numReadMessages =
        result.setCoder(HL7v2MessageCoder.of()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo(NUM_ADT);

    PAssert.that(result)
        .satisfies(
            input -> {
              for (HL7v2Message elem : input) {
                assertEquals("ADT", elem.getMessageType());
              }
              return null;
            });

    pipeline.run();
  }
}
