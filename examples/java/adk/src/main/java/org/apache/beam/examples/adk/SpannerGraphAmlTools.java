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

import com.google.adk.tools.Annotations;
import com.google.cloud.spanner.*;
import io.opentelemetry.api.GlobalOpenTelemetry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("initialization.field.uninitialized")
public class SpannerGraphAmlTools implements Serializable {

  private transient DatabaseClient dbClient;
  private transient Spanner spanner;

  public void initSpanner(String instanceId, String databaseId) {

    SpannerOptions options =
        SpannerOptions.newBuilder()
            .setOpenTelemetry(GlobalOpenTelemetry.get())
            .setEnableEndToEndTracing(true)
            .setEnableExtendedTracing(true)
            .build();

    this.spanner = options.getService();
    DatabaseId db = DatabaseId.of(options.getProjectId(), instanceId, databaseId);
    this.dbClient = spanner.getDatabaseClient(db);
  }

  public void close() {
    if (spanner != null) {
      spanner.close();
    }
  }

  // --- Tool 1: Circular Flow ---
  public String detectCircularFlow(@Annotations.Schema(name = "userId") String userId) {
    String gqlQuery =
        "GRAPH FinancialGraph "
            + "MATCH (a:Account {AccountId: @user_id})-[t1:TRANSFERRED_TO]->(b:Account) "
            + "      -[t2:TRANSFERRED_TO]->(c:Account) "
            + "      -[t3:TRANSFERRED_TO]->(a) "
            + "WHERE TIMESTAMP_DIFF(t3.Timestamp, t1.Timestamp, HOUR) <= 48 "
            + "RETURN b.AccountId AS Hop1, c.AccountId AS Hop2, t1.Amount AS InitialAmount, t3.Amount AS ReturnedAmount";

    Statement statement = Statement.newBuilder(gqlQuery).bind("user_id").to(userId).build();

    List<String> findings = new ArrayList<>();
    try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
      while (rs.next()) {
        findings.add(
            String.format(
                "Loop detected via %s -> %s (Initial: $%.2f, Returned: $%.2f)",
                rs.getString("Hop1"),
                rs.getString("Hop2"),
                rs.getBigDecimal("InitialAmount"),
                rs.getBigDecimal("ReturnedAmount")));
      }
    }

    return findings.isEmpty()
        ? "NO_CIRCULAR_FLOW"
        : "SUSPICIOUS_CIRCULAR_FLOW: " + String.join("; ", findings);
  }

  // --- Tool 2: Fan-In Structuring ---
  public String detectFanInStructuring(
      @Annotations.Schema(name = "targetUserId") String targetUserId) {
    String gqlQuery =
        "select * from GRAPH_TABLE(\n"
            + "FinancialGraph \n"
            + "MATCH (sender:Account)-[t:TRANSFERRED_TO]->(collector:Account {AccountId: @target_user_id}) \n"
            + "      WHERE t.Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) \n"
            + "            AND t.Amount BETWEEN 2000 AND 9999 \n"
            + "      WITH collector, \n"
            + "           COUNT(DISTINCT sender) AS unique_senders, \n"
            + "           SUM(t.Amount) AS total_funneled \n"
            + "      \n"
            + "      RETURN unique_senders, total_funneled ) as gt where unique_senders >= 3 ";

    Statement statement =
        Statement.newBuilder(gqlQuery).bind("target_user_id").to(targetUserId).build();

    try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
      if (rs.next()) {
        return String.format(
            "SUSPICIOUS_FAN_IN: Funneled by %d unique accounts, Total: $%.2f",
            rs.getLong("unique_senders"), rs.getBigDecimal("total_funneled"));
      }
    }

    return "NO_FAN_IN_DETECTED";
  }

  // --- Tool 3: Shared Identity ---
  public String detectSharedIdentity(
      @Annotations.Schema(name = "senderId") String senderId,
      @Annotations.Schema(name = "receiverId") String receiverId) {
    String gqlQuery =
        "GRAPH FinancialGraph "
            + "MATCH (acc1:Account {AccountId: @sender_id})-[t:TRANSFERRED_TO]->(acc2:Account {AccountId: @receiver_id}) "
            + "MATCH (acc1)-[:USED_DEVICE]->(shared_node)<-[:USED_DEVICE]-(acc2) "
            + "RETURN LABELS(shared_node)[0] AS SharedAttributeType, shared_node.DeviceId AS SharedAttributeValue";

    Statement statement =
        Statement.newBuilder(gqlQuery)
            .bind("sender_id")
            .to(senderId)
            .bind("receiver_id")
            .to(receiverId)
            .build();

    List<String> sharedElements = new ArrayList<>();
    try (ResultSet rs = dbClient.singleUse().executeQuery(statement)) {
      while (rs.next()) {
        sharedElements.add(
            rs.getString("SharedAttributeType") + ":" + rs.getString("SharedAttributeValue"));
      }
    }

    return sharedElements.isEmpty()
        ? "NO_SHARED_IDENTITY"
        : "SYNTHETIC_CLUSTER_ALERT: Shared " + String.join(", ", sharedElements);
  }
}
