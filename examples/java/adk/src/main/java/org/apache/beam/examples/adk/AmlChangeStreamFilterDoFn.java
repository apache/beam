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
package org.apache.beam.examples.adk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;

public class AmlChangeStreamFilterDoFn extends DoFn<DataChangeRecord, TransactionEvent> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ProcessElement
  public void processElement(@Element DataChangeRecord record, OutputReceiver<TransactionEvent> out)
      throws Exception {
    if (record.getModType() == ModType.INSERT) {
      for (Mod mod : record.getMods()) {
        String keysJson = mod.getKeysJson();
        String newValuesJson = mod.getNewValuesJson();
        if (keysJson != null && newValuesJson != null) {
          JsonNode keys = MAPPER.readTree(keysJson);
          JsonNode newValues = MAPPER.readTree(newValuesJson);
          System.out.println("values " + newValuesJson);
          String transactionId =
              keys.has("TransactionId") ? keys.get("TransactionId").asText() : "";
          String senderId = newValues.has("SenderId") ? newValues.get("SenderId").asText() : "";
          String receiverId =
              newValues.has("ReceiverId") ? newValues.get("ReceiverId").asText() : "";
          BigDecimal amount =
              newValues.has("Amount")
                  ? new BigDecimal(newValues.get("Amount").asText())
                  : BigDecimal.ZERO;
          String status = newValues.has("Status") ? newValues.get("Status").asText() : null;
          String riskReason =
              newValues.has("RiskReason") ? newValues.get("RiskReason").asText() : null;
          String timestamp = newValues.has("Timestamp") ? newValues.get("Timestamp").asText() : "";
          String reviewedAt =
              newValues.has("ReviewedAt") ? newValues.get("ReviewedAt").asText() : null;
          TransactionEvent value =
              TransactionEvent.create(
                  transactionId,
                  senderId,
                  receiverId,
                  amount,
                  status,
                  riskReason,
                  timestamp,
                  reviewedAt);
          System.out.println("new transaction - " + value);

          out.output(value);
        }
      }
    }
  }
}
