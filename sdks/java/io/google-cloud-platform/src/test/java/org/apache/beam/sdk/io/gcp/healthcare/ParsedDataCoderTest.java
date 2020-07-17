package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1beta1.model.Message;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2MessageCoderTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void test_HL7vMessageCoder_schematizedDataEncode() {
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

  @Test
  public void test_HL7vMessageCoder_parsedDataEncode() {
  }

  @Test
  public void test_HL7vMessageCoder_parsedDataDecode() {
  }


}
