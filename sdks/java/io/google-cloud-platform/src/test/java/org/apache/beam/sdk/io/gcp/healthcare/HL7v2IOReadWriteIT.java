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
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.deleteAllHL7v2Messages;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.writeHL7v2Messages;

import com.google.api.services.healthcare.v1.model.Message;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.ListHL7v2MessageIDs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This should catch that we read {@link HL7v2Message} with schematized data but do not fail inserts
 * with schematized data which should be output only.
 */
@RunWith(JUnit4.class)
public class HL7v2IOReadWriteIT {

  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String BASE =
      "hl7v2_store_rw_it_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
  private static final String INPUT_HL7V2_STORE_NAME = BASE + "INPUT";
  private static final String OUTPUT_HL7V2_STORE_NAME = BASE + "OUTPUT";
  private static final String METADATA = "Metadata for message";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void createHL7v2tores() throws IOException {
    String project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.createHL7v2Store(healthcareDataset, INPUT_HL7V2_STORE_NAME);
    client.createHL7v2Store(healthcareDataset, OUTPUT_HL7V2_STORE_NAME);
  }

  @AfterClass
  public static void deleteHL7v2tores() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteHL7v2Store(healthcareDataset + "/hl7V2Stores/" + INPUT_HL7V2_STORE_NAME);
    client.deleteHL7v2Store(healthcareDataset + "/hl7V2Stores/" + OUTPUT_HL7V2_STORE_NAME);
  }

  @Before
  public void setup() throws Exception {
    if (client == null) {
      client = new HttpHealthcareApiClient();
    }

    // Create HL7 messages and write them to HL7v2 Store.
    writeHL7v2Messages(this.client, healthcareDataset + "/hl7V2Stores/" + INPUT_HL7V2_STORE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllHL7v2Messages(client, healthcareDataset + "/hl7V2Stores/" + OUTPUT_HL7V2_STORE_NAME);
  }

  @Test
  public void testHL7v2IOE2E() throws Exception {
    HL7v2IO.Read.Result readResult =
        pipeline
            .apply(
                new ListHL7v2MessageIDs(
                    Collections.singletonList(
                        healthcareDataset + "/hl7V2Stores/" + INPUT_HL7V2_STORE_NAME)))
            .apply(HL7v2IO.getAll());

    PCollection<Long> numReadMessages =
        readResult.getMessages().setCoder(HL7v2MessageCoder.of()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo((long) MESSAGES.size());
    PAssert.that(readResult.getFailedReads()).empty();

    HL7v2IO.Write.Result writeResult =
        readResult
            .getMessages()
            .apply(
                HL7v2IO.ingestMessages(
                    healthcareDataset + "/hl7V2Stores/" + OUTPUT_HL7V2_STORE_NAME));

    PAssert.that(writeResult.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();

    try {
      HL7v2IOTestUtil.waitForHL7v2Indexing(
          client,
          healthcareDataset + "/hl7V2Stores/" + OUTPUT_HL7V2_STORE_NAME,
          MESSAGES.size(),
          Duration.standardMinutes(10));
    } catch (TimeoutException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testHL7v2IOGetAllE2E() {
    HL7v2IO.Read.Result actualResult =
        pipeline
            .apply(
                new ListHL7v2MessageIDs(
                    Collections.singletonList(
                        healthcareDataset + "/hl7V2Stores/" + INPUT_HL7V2_STORE_NAME)))
            .apply(HL7v2IO.getAll());
    PCollection<HL7v2Message> actualMessages =
        actualResult
            .getMessages()
            .apply(
                "Convert to message to contains only data parameter for assert",
                ParDo.of(new MapHl7MessagesToComparableMessages()));

    PAssert.that(actualMessages).containsInAnyOrder(MESSAGES);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testHL7v2IOGetAllByReadParameterE2E() {
    PCollection<String> msgIds =
        pipeline.apply(
            new ListHL7v2MessageIDs(
                Collections.singletonList(
                    healthcareDataset + "/hl7V2Stores/" + INPUT_HL7V2_STORE_NAME)));
    PCollection<HL7v2ReadParameter> readParameters =
        msgIds.apply("Make read parameter lists", ParDo.of(new MapIdsToReadParameter()));
    HL7v2IO.HL7v2Read.Result result = readParameters.apply(HL7v2IO.readAllRequests());
    PCollection<HL7v2ReadResponse> actualResponse =
        result
            .getMessages()
            .setCoder(HL7v2ReadResponseCoder.of())
            .apply(ParDo.of(new MapHL7v2ReadResponseToComparableResponse()));

    List<HL7v2ReadResponse> expectedResponse =
        MESSAGES.stream()
            .map((HL7v2Message message) -> HL7v2ReadResponse.of(METADATA, message))
            .collect(Collectors.toList());

    PAssert.that(actualResponse).containsInAnyOrder(expectedResponse);
    pipeline.run().waitUntilFinish();
  }

  private static class MapHl7MessagesToComparableMessages extends DoFn<HL7v2Message, HL7v2Message> {

    @ProcessElement
    public void processElement(@Element HL7v2Message input, OutputReceiver<HL7v2Message> receiver) {
      Message message = new Message();
      message.setData(input.getData());
      receiver.output(HL7v2Message.fromModel(message));
    }
  }

  private static class MapIdsToReadParameter extends DoFn<String, HL7v2ReadParameter> {

    @ProcessElement
    public void processElement(@Element String msgId, OutputReceiver<HL7v2ReadParameter> receiver) {
      HL7v2ReadParameter parameter = HL7v2ReadParameter.of(METADATA, msgId);
      receiver.output(parameter);
    }
  }

  private static class MapHL7v2ReadResponseToComparableResponse
      extends DoFn<HL7v2ReadResponse, HL7v2ReadResponse> {

    @ProcessElement
    public void processElement(
        @Element HL7v2ReadResponse input, OutputReceiver<HL7v2ReadResponse> receiver) {
      Message message = new Message();
      message.setData(input.getHL7v2Message().getData());
      HL7v2ReadResponse response =
          HL7v2ReadResponse.of(input.getMetadata(), HL7v2Message.fromModel(message));
      receiver.output(response);
    }
  }
}
