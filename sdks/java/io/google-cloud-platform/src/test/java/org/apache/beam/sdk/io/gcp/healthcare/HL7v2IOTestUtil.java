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

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class HL7v2IOTestUtil {
  /** Clear all messages from the HL7v2 store. */
  static void deleteAllHL7v2Messages(HealthcareApiClient client, HL7v2IOTestOptions options)
      throws IOException {
    List<IOException> deleteErrors = new ArrayList<>();
    client
        .getHL7v2MessageIDStream(options.getHl7v2Store())
        .forEach(
            (String msgID) -> {
              try {
                client.deleteHL7v2Message(msgID);
              } catch (IOException e) {
                e.printStackTrace();
                deleteErrors.add(e);
              }
            });

    if (deleteErrors.size() > 0) {
      throw deleteErrors.get(0);
    }
  }

  /** Populate the test messages into the HL7v2 store. */
  static void writeHL7v2Messages(
      HealthcareApiClient client, HL7v2IOTestOptions options, long numMessages) throws IOException {
    Message msg = new Message();

    for (int i = 0; i < numMessages; i++) {
      // TODO modify each new message or read from resource file.
      client.createHL7v2Message(options.getHl7v2Store(), msg);
    }
  }
}
