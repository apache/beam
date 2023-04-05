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

import com.google.api.services.healthcare.v1.model.Message;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testHL7v2IOFailedReads() {
    List<String> badMessageIDs =
        Arrays.asList(
            "projects/a/locations/b/datasets/c/hl7V2Stores/d/messages/foo",
            "projects/a/locations/b/datasets/c/hl7V2Stores/d/messages/bar");
    HL7v2IO.Read.Result readResult =
        pipeline.apply(Create.of(badMessageIDs)).apply(HL7v2IO.getAll());

    PCollection<HealthcareIOError<String>> failed = readResult.getFailedReads();

    PCollection<HL7v2Message> messages = readResult.getMessages();

    PCollection<String> failedMsgIds =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedMsgIds).containsInAnyOrder(badMessageIDs);
    PAssert.that(messages).empty();
    pipeline.run();
  }

  @Test
  public void testHL7v2IOFailedReadsByParameter() {
    List<HL7v2ReadParameter> badReadParameters =
        Arrays.asList(
            HL7v2ReadParameter.of(
                "metadata-foo", "projects/a/locations/b/datasets/c/hl7V2Stores/d/messages/foo"),
            HL7v2ReadParameter.of(
                "metadata-bar", "projects/a/locations/b/datasets/c/hl7V2Stores/d/messages/bar"));

    PCollection<HL7v2ReadParameter> parameters = pipeline.apply(Create.of(badReadParameters));
    HL7v2IO.HL7v2Read.Result readResult = parameters.apply(HL7v2IO.readAllRequests());

    PCollection<HealthcareIOError<HL7v2ReadParameter>> failed = readResult.getFailedReads();

    PCollection<HL7v2ReadResponse> messages = readResult.getMessages();

    PCollection<HL7v2ReadParameter> failedParameters =
        failed.apply("Map to parameters", ParDo.of(new MapHealthCareIOErrorToReadParameter()));

    PAssert.that(failedParameters).containsInAnyOrder(badReadParameters);
    PAssert.that(messages).empty();
    pipeline.run();
  }

  @Test
  public void testHL7v2IOFailedWrites() {
    Message msg = new Message().setData("");
    List<HL7v2Message> emptyMessages = Collections.singletonList(HL7v2Message.fromModel(msg));

    PCollection<HL7v2Message> messages =
        pipeline.apply(Create.of(emptyMessages).withCoder(new HL7v2MessageCoder()));

    HL7v2IO.Write.Result writeResult =
        messages.apply(
            HL7v2IO.ingestMessages(
                "projects/foo/locations/us-central1/datasets/bar/hl7V2Stores/baz"));

    PCollection<HealthcareIOError<HL7v2Message>> failedInserts =
        writeResult.getFailedInsertsWithErr();

    PCollection<Long> failedMsgs = failedInserts.apply(Count.globally());

    PAssert.thatSingleton(failedMsgs).isEqualTo(1L);

    pipeline.run();
  }

  private static class MapHealthCareIOErrorToReadParameter
      extends DoFn<HealthcareIOError<HL7v2ReadParameter>, HL7v2ReadParameter> {

    @ProcessElement
    public void processElement(
        @Element HealthcareIOError<HL7v2ReadParameter> error,
        OutputReceiver<HL7v2ReadParameter> receiver) {
      receiver.output(error.getDataResource());
    }
  }
}
