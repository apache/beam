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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewTransactionDoFn extends DoFn<PubsubMessage, Mutation> {

  private final String outputTable;
  @MonotonicNonNull ObjectMapper mapper = null;
  private transient @MonotonicNonNull Tracer tracer = null;
  private static final Logger LOG = LoggerFactory.getLogger(NewTransactionDoFn.class);

  public NewTransactionDoFn(String outputTable) {
    this.outputTable = outputTable;
  }

  // POJO representing the incoming JSON structure
  public static class TransactionPayload {
    @JsonProperty("TransactionId")
    private @Nullable String transactionId;

    @JsonProperty("SenderId")
    private @Nullable String senderId;

    @JsonProperty("ReceiverId")
    private @Nullable String receiverId;

    @JsonProperty("Amount")
    private double amount;

    @JsonProperty("Timestamp")
    private @Nullable String timestamp;

    // Getters and Setters
    public @Nullable String getTransactionId() {
      return transactionId;
    }

    public void setTransactionId(String transactionId) {
      this.transactionId = transactionId;
    }

    public @Nullable String getSenderId() {
      return senderId;
    }

    public void setSenderId(String senderId) {
      this.senderId = senderId;
    }

    public @Nullable String getReceiverId() {
      return receiverId;
    }

    public void setReceiverId(String receiverId) {
      this.receiverId = receiverId;
    }

    public double getAmount() {
      return amount;
    }

    public void setAmount(double amount) {
      this.amount = amount;
    }

    public @Nullable String getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(String timestamp) {
      this.timestamp = timestamp;
    }
  }

  @Setup
  public void setup(PipelineOptions options) {
    mapper = new ObjectMapper();
    this.tracer =
        options
            .as(SdkHarnessOptions.class)
            .getOpenTelemetry()
            .getTracer("org.apache.beam.examples.adk.aml");
  }

  @ProcessElement
  public void processElement(@Element PubsubMessage in, OutputReceiver<Mutation> out)
      throws Exception {
    Span parentSpan =
        Preconditions.checkStateNotNull(tracer)
            .spanBuilder("NewTransaction.Process")
            .setAttribute("msgId", in.getMessageId())
            .startSpan();

    try (Scope ignored = parentSpan.makeCurrent()) {
      String payload = new String(in.getPayload(), java.nio.charset.StandardCharsets.UTF_8);

      // Deserialize JSON payload directly into the TransactionPayload POJO
      TransactionPayload tx =
          Preconditions.checkStateNotNull(mapper).readValue(payload, TransactionPayload.class);
      // Update transaction with RiskReason and ReviewedAt
      Mutation mutation =
          Mutation.newInsertOrUpdateBuilder(outputTable)
              .set("TransactionId")
              .to(UUID.randomUUID().toString())
              .set("SenderId")
              .to(tx.getSenderId())
              .set("ReceiverId")
              .to(tx.getReceiverId())
              .set("Amount")
              .to(new BigDecimal(tx.getAmount()))
              .set("Status")
              .to("PENDING")
              .set("Timestamp")
              .to(
                  com.google.cloud.Timestamp.parseTimestamp(
                      Objects.requireNonNullElse(tx.getTimestamp(), "")))
              .build();
      LOG.info("mutating {}", mutation);
      out.output(mutation);

    } catch (Exception e) {
      parentSpan.recordException(e);
      throw e;
    } finally {
      parentSpan.end();
    }
  }
}
