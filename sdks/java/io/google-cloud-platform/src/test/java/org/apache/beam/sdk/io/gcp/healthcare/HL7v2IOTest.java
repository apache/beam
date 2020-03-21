package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void test_HL7v2IO_failedReads() {
    List<String> badMessageIDs = Arrays.asList("foo", "bar");
    HL7v2IO.Read.Result readResult = pipeline
        .apply(Create.of(badMessageIDs))
        .apply(HL7v2IO.readAll());
    PCollection<HealthcareIOError<String>> failed = readResult.getFailedReads();
    PCollection<Message> messages = readResult.getMessages().setCoder(new MessageCoder());
    PCollection<String> failedMsgIds = failed.apply(
        MapElements.into(TypeDescriptors.strings()).via((HealthcareIOError::getDataResource)));

    PAssert.that(failedMsgIds).containsInAnyOrder(badMessageIDs);
    PAssert.that(messages).empty();
    pipeline.run();
  }

  @Test
  public void test_HL7v2IO_failedWrites() {
    Message msg = new Message().setData("");
    List<Message> emptyMessages = Collections.singletonList(msg);
    PCollection<Message> messages = pipeline.apply(Create.of(emptyMessages).withCoder(new MessageCoder()));
    HL7v2IO.Write.Result writeResult = messages.apply(HL7v2IO.ingestMessages("projects/foo/locations/us-central1/datasets/bar/hl7V2Stores/baz"));
    PCollection<HealthcareIOError<Message>> failedInserts = writeResult.getFailedInsertsWithErr();
    PCollection<Message> failedMsgs = failedInserts.apply(
        MapElements.into(TypeDescriptor.of(Message.class))
            .via((HealthcareIOError::getDataResource))).setCoder(new MessageCoder());
    PAssert.that(failedMsgs).containsInAnyOrder(emptyMessages);
    pipeline.run();
  }

}
