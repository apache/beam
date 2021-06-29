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

import com.google.api.client.util.Base64;
import com.google.api.client.util.Sleeper;
import com.google.api.services.healthcare.v1beta1.model.Message;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HL7v2MessagePages;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

class HL7v2IOTestUtil {
  public static final long HL7V2_INDEXING_TIMEOUT_MINUTES = 10L;
  /** Google Cloud Healthcare Dataset in Apache Beam integration test project. */
  public static final String HEALTHCARE_DATASET_TEMPLATE =
      "projects/%s/locations/us-central1/datasets/apache-beam-integration-testing";

  // Could generate more messages at scale using a tool like
  // https://synthetichealth.github.io/synthea/ if necessary chose not to avoid the dependency.
  static final List<String> MESSAGES_DATA =
      Arrays.asList(
          // ADT Message.
          "MSH|^~\\&|CERNER|RAL|STREAMS|RAL|20190309132444||ADT^A01|827|T|2.3|||AL||44|ASCII\r"
              + "EVN|A01|20190309132444|||C184726198^Connell^Alistair^^^Dr.^^^DRNBR^PRSNL^^^ORGDR|\r"
              + "PID|1|456656825^^^SIMULATOR MRN^MRN|456656825^^^SIMULATOR MRN^MRN~1495641465^^^NHSNBR^NHSNMBR||Doe^Jane^Glynis^^Miss^^CURRENT||19940703010000|2|||73 Alto Road^^London^^HG63 4SN^GBR^HOME||075 6368 2928^HOME|||||||||A^White - British^||||||||\r"
              + "PD1|||YIEWSLEY FAMILY PRACTICE^^6010|||||\r"
              + "PV1|1|INPATIENT|Orthopaedic^Bay A^Bed 11^Simulated Hospital^^BED^Orthopaedic ward^1|28b|||C184726198^Connell^Alistair^^^Dr.^^^DRNBR^PRSNL^^^ORGDR|||180|||||||||12611791848783219197^^^^visitid||||||||||||||||||||||ARRIVED|||20190309132444||\r"
              + "AL1|0|allergy|Z88.0^Personal history of allergy to penicillin^ZAL|SEVERE|Shortness of breath|\r"
              + "AL1|1|allergy|T63.441A^Toxic effect of venom of bees^ZAL|MODERATE|Vomiting|\r"
              + "AL1|2|allergy|Z91.013^Personal history of allergy to sea food^ZAL|SEVERE|Swollen face|\r"
              + "AL1|3|allergy|Z91.040^Latex allergy^ZAL|MODERATE|Raised, itchy, red rash|",
          // Another ADT Message
          "MSH|^~\\&|hl7Integration|hl7Integration|||20190309132544||ADT^A08|||2.5|\r"
              + "EVN|A01|20130617154644||foo\r"
              + "PID|1|465 306 5961||407623|Wood^Patrick^^^MR||19700101|1|||High Street^^Oxford^^Ox1 4DP~George St^^Oxford^^Ox1 5AP|||||||\r"
              + "NK1|1|Wood^John^^^MR|Father||999-9999\r"
              + "NK1|2|Jones^Georgie^^^MSS|MOTHER||999-9999\r"
              + "PV1|1||Location||||||||||||||||261938_6_201306171546|||||||||||||||||||||||||20130617134644|||||||||",

          // Not an ADT message.
          "MSH|^~\\&|ULTRA|TML|OLIS|OLIS|201905011130||ORU^R01|20169838-v25|T|2.5\r"
              + "PID|||7005728^^^TML^MR||TEST^RACHEL^DIAMOND||19310313|F|||200 ANYWHERE ST^^TORONTO^ON^M6G 2T9||(416)888-8888||||||1014071185^KR\r"
              + "PV1|1||OLIS||||OLIST^BLAKE^DONALD^THOR^^^^^921379^^^^OLIST\r"
              + "ORC|RE||T09-100442-RET-0^^OLIS_Site_ID^ISO|||||||||OLIST^BLAKE^DONALD^THOR^^^^L^921379\r"
              + "OBR|0||T09-100442-RET-0^^OLIS_Site_ID^ISO|RET^RETICULOCYTE COUNT^HL79901 literal|||200905011106|||||||200905011106||OLIST^BLAKE^DONALD^THOR^^^^L^921379||7870279|7870279|T09-100442|MOHLTC|200905011130||B7|F||1^^^200905011106^^R\r"
              + "OBX|1|ST|||Test Value");

  static final List<HL7v2Message> MESSAGES =
      MESSAGES_DATA.stream()
          .map(String::getBytes)
          .map(Base64::encodeBase64String)
          .map(
              (String data) -> {
                Message msg = new Message();
                msg.setData(data);
                return HL7v2Message.fromModel(msg);
              })
          .collect(Collectors.toList());

  static final long NUM_ADT = 2;
  /** Clear all messages from the HL7v2 store. */
  static void deleteAllHL7v2Messages(HealthcareApiClient client, String hl7v2Store)
      throws IOException {
    for (List<HL7v2Message> page : new HL7v2MessagePages(client, hl7v2Store, null, null)) {
      for (String msgId : page.stream().map(HL7v2Message::getName).collect(Collectors.toList())) {
        client.deleteHL7v2Message(msgId);
      }
    }
  }

  /** Utiliy for waiting on HL7v2 Store indexing to be complete see BEAM-9779. */
  public static void waitForHL7v2Indexing(
      HealthcareApiClient client, String hl7v2Store, long expectedNumMessages, Duration timeout)
      throws InterruptedException, TimeoutException {

    Instant start = Instant.now();
    long sleepMs = 50;
    long numListedMessages = 0;
    while (new Duration(start, Instant.now()).isShorterThan(timeout)) {
      numListedMessages = 0;
      // count messages in HL7v2 Store.
      for (List<HL7v2Message> page :
          new HttpHealthcareApiClient.HL7v2MessagePages(client, hl7v2Store, null, null)) {
        numListedMessages += page.size();
      }
      if (numListedMessages == expectedNumMessages) {
        return;
      }
      // exponential backoff.
      sleepMs *= 2;
      // exit if next sleep will violate timeout
      if (new Duration(start, Instant.now()).plus(sleepMs).isShorterThan(timeout)) {
        Sleeper.DEFAULT.sleep(sleepMs);
      } else {
        throw new TimeoutException(
            String.format(
                "Timed out waiting for %s to reach %s messages. last list request returned %s messages.",
                hl7v2Store, expectedNumMessages, numListedMessages));
      }
    }
  }

  /** Populate the test messages into the HL7v2 store. */
  static void writeHL7v2Messages(HealthcareApiClient client, String hl7v2Store)
      throws IOException, InterruptedException, TimeoutException {
    for (HL7v2Message msg : MESSAGES) {
      client.createHL7v2Message(hl7v2Store, msg.toModel());
    }
    // [BEAM-9779] HL7v2 indexing is asyncronous. Block until indexing completes to stabilize this
    // IT.
    HL7v2IOTestUtil.waitForHL7v2Indexing(
        client,
        hl7v2Store,
        MESSAGES.size(),
        Duration.standardMinutes(HL7V2_INDEXING_TIMEOUT_MINUTES));
  }

  /**
   * List HL7v2 message IDs. This is just convenient for integration testing the unbounded
   * HL7v2IO.Read without the dependency on PubSub Notification channel.
   */
  static class ListHL7v2MessageIDs extends PTransform<PBegin, PCollection<String>> {

    private final List<String> hl7v2Stores;
    private final String filter;

    /**
     * Instantiates a new List HL7v2 message IDs with filter.
     *
     * @param hl7v2Stores the HL7v2 stores
     * @param filter the filter
     */
    ListHL7v2MessageIDs(List<String> hl7v2Stores, String filter) {
      this.hl7v2Stores = hl7v2Stores;
      this.filter = filter;
    }

    /**
     * Instantiates a new List HL7v2 message IDs without filter.
     *
     * @param hl7v2Stores the HL7v2 stores
     */
    ListHL7v2MessageIDs(List<String> hl7v2Stores) {
      this.hl7v2Stores = hl7v2Stores;
      this.filter = null;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(Create.of(this.hl7v2Stores))
          .apply(ParDo.of(new ListHL7v2MessageIDsFn(this.filter)));
    }
  }

  /** The type List HL7v2 fn. */
  static class ListHL7v2MessageIDsFn extends DoFn<String, String> {

    private final String filter;
    private transient HealthcareApiClient client;

    /**
     * Instantiates a new List HL7v2 fn.
     *
     * @param filter the filter
     */
    ListHL7v2MessageIDsFn(String filter) {
      this.filter = filter;
    }

    /**
     * Init client.
     *
     * @throws IOException the io exception
     */
    @Setup
    public void initClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    /**
     * List messages.
     *
     * @param context the context
     * @throws IOException the io exception
     */
    @ProcessElement
    public void listMessages(ProcessContext context) throws IOException {
      String hl7v2Store = context.element();
      // Output all elements of all pages.
      HttpHealthcareApiClient.HL7v2MessagePages pages =
          new HttpHealthcareApiClient.HL7v2MessagePages(
              client, hl7v2Store, null, null, this.filter, "sendTime");
      for (List<HL7v2Message> page : pages) {
        page.stream().map(HL7v2Message::getName).forEach(context::output);
      }
    }
  }

  static Message testMessage(String name) {
    Message msg = new Message();
    msg.setName(name);
    return msg;
  }
}
