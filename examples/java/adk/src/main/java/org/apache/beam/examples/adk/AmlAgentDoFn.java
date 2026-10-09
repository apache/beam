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

import com.google.adk.agents.LlmAgent;
import com.google.adk.events.Event;
import com.google.adk.models.Gemini;
import com.google.adk.runner.InMemoryRunner;
import com.google.adk.sessions.Session;
import com.google.adk.tools.FunctionTool;
import com.google.cloud.spanner.Mutation;
import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionResponse;
import com.google.genai.types.Part;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("initialization.fields.uninitialized")
public class AmlAgentDoFn extends DoFn<KV<String, TransactionEvent>, Mutation> {

  private final String instanceId;
  private final String databaseId;
  private final String outputTable;

  private transient SpannerGraphAmlTools graphTools;
  private transient LlmAgent amlAgent;
  private transient Tracer tracer;
  private transient InMemoryRunner runner;
  private transient Client client;

  @StateId("memory")
  private final StateSpec<ValueState<Integer>> valueStateSpec = StateSpecs.value(VarIntCoder.of());

  public AmlAgentDoFn(String instanceId, String databaseId, String outputTable) {
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.outputTable = outputTable;
  }

  @Setup
  public void setup(PipelineOptions options) {
    // 1. Initialize DB and Spanner Graph Tools
    this.graphTools = new SpannerGraphAmlTools();
    this.graphTools.initSpanner(instanceId, databaseId);
    this.tracer =
        options
            .as(SdkHarnessOptions.class)
            .getOpenTelemetry()
            .getTracer("org.apache.beam.examples.adk.aml");

    // 2. Configure System Instructions for the Agent
    String systemInstruction =
        "You are an Anti-Money Laundering (AML) Investigator AI Agent.\n"
            + "Given a transaction event, execute Spanner Graph (GQL) tools to uncover potential fraud rings:\n"
            + "1. Run 'detectCircularFlow' using the sender ID to uncover round-tripping money loops.\n"
            + "2. Run 'detectFanInStructuring' using the receiver ID to check for smurfing/fan-in aggregation.\n"
            + "3. Run 'detectSharedIdentity' between sender and receiver to identify synthetic/co-located accounts.\n"
            + "4. Consolidate findings and output a raw JSON object with exactly two string fields (no markdown formatting):\n"
            + " - \"status\": The Risk Assessment ('CLEARED', 'REVIEW_REQUIRED', or 'PENDING').\n"
            + " - \"riskReason\": The detailed justification.";

    // 3. Build ADK LlmAgent with registered tools
    FunctionTool circularFlowTool = FunctionTool.create(graphTools, "detectCircularFlow");
    FunctionTool fanInTool = FunctionTool.create(graphTools, "detectFanInStructuring");
    FunctionTool sharedIdentityTool = FunctionTool.create(graphTools, "detectSharedIdentity");
    client =
        Client.builder()
            .vertexAI(true)
            .project(options.as(GcpOptions.class).getProject())
            .location("us")
            .build();
    this.amlAgent =
        LlmAgent.builder()
            .name("AmlGraphInvestigator")
            .model(new Gemini("gemini-3.5-flash", client))
            .description(
                "Investigates financial transaction records using Spanner GQL Graph Queries.")
            .instruction(systemInstruction)
            .tools(circularFlowTool, fanInTool, sharedIdentityTool)
            .build();

    this.runner = new InMemoryRunner(this.amlAgent);
  }

  @Teardown
  public void tearDown() {
    if (graphTools != null) {
      graphTools.close();
    }
    if (client != null) {
      client.close();
    }
  }

  private static void processAgentEvent(
      Event event,
      StringBuilder agentResponseBuilder,
      AtomicBoolean toolCalledInTurn,
      AtomicBoolean toolErroredInTurn) {
    if (event.content().isPresent()) {
      event
          .content()
          .get()
          .parts()
          .ifPresent(
              parts -> {
                for (Part part : parts) {
                  if (part.text().isPresent()) {
                    System.out.print(part.text().get());
                    agentResponseBuilder.append(part.text().get());
                  }
                  if (part.functionCall().isPresent()) {
                    toolCalledInTurn.set(true);
                  }
                  if (part.functionResponse().isPresent()) {
                    FunctionResponse fr = part.functionResponse().get();
                    fr.response()
                        .ifPresent(
                            responseMap -> {
                              if (responseMap.containsKey("error")
                                  || (responseMap.containsKey("status")
                                      && "error"
                                          .equalsIgnoreCase(
                                              String.valueOf(responseMap.get("status"))))) {
                                toolErroredInTurn.set(true);
                              }
                            });
                  }
                }
              });
    }
    if (event.errorCode().isPresent() || event.errorMessage().isPresent()) {
      toolErroredInTurn.set(true);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, TransactionEvent> pairOfUserAndTx,
      @StateId("memory") ValueState<Integer> valueState,
      MultiOutputReceiver out)
      throws Exception {
    TransactionEvent tx = pairOfUserAndTx.getValue();

    Integer memory = valueState.read();
    if (memory == null) {
      memory = 0;
    }
    valueState.write(memory + 1);

    Span parentSpan =
        tracer
            .spanBuilder("invoke_agent:AmlAgentWorker")
            .setAttribute("transaction.id", tx.getTransactionId())
            .startSpan();

    try (Scope parentScope = parentSpan.makeCurrent()) {
      String prompt =
          String.format(
              "Investigate TransactionID: %s, Sender: %s, Receiver: %s, Amount: $%.2f, Timestamp: %s",
              tx.getTransactionId(),
              tx.getSenderId(),
              tx.getReceiverId(),
              tx.getAmount(),
              tx.getTimestamp());

      // Create a unique session per transaction
      Session session =
          runner
              .sessionService()
              .createSession(amlAgent.name(), "user-" + tx.getSenderId(), null, null)
              .blockingGet();

      Content userMsg = Content.fromParts(Part.fromText(prompt));

      final StringBuilder agentResponseBuilder = new StringBuilder();
      final AtomicBoolean toolCalledInTurn = new AtomicBoolean(false);
      final AtomicBoolean toolErroredInTurn = new AtomicBoolean(false);

      // ADK's LlmAgent executes reasoning, tool execution loops, and final output automatically
      runner
          .runAsync("user-" + tx.getSenderId(), session.id(), userMsg)
          .blockingForEach(
              event ->
                  processAgentEvent(
                      event, agentResponseBuilder, toolCalledInTurn, toolErroredInTurn));

      String finalAssessment = agentResponseBuilder.toString();

      String parsedStatus = "REVIEW_REQUIRED";
      String parsedRiskReason = finalAssessment;

      try {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
            new com.fasterxml.jackson.databind.ObjectMapper();
        // Remove potential markdown formatting returned by LLMs
        String cleanedJson = finalAssessment.replaceAll("```json", "").replaceAll("```", "").trim();
        com.fasterxml.jackson.databind.JsonNode jsonNode = mapper.readTree(cleanedJson);
        if (jsonNode.has("status")) {
          parsedStatus = jsonNode.get("status").asText();
        }
        if (jsonNode.has("riskReason")) {
          parsedRiskReason = jsonNode.get("riskReason").asText();
        }
      } catch (Exception ex) {
        System.err.println("Failed to parse agent response as JSON: " + finalAssessment);
      }

      // Update transaction with RiskReason and ReviewedAt
      Mutation mutation =
          Mutation.newUpdateBuilder(outputTable)
              .set("TransactionId")
              .to(tx.getTransactionId())
              .set("SenderId")
              .to(tx.getSenderId())
              .set("ReceiverId")
              .to(tx.getReceiverId())
              .set("Amount")
              .to(tx.getAmount())
              .set("Status")
              .to(parsedStatus)
              .set("Timestamp")
              .to(com.google.cloud.Timestamp.parseTimestamp(tx.getTimestamp()))
              .set("RiskReason")
              .to(parsedRiskReason)
              .set("ReviewedAt")
              .to(com.google.cloud.Timestamp.now())
              .build();
      System.out.println("mutating: " + mutation);
      out.get(AmlPipeline.MUTATION_TAG).output(mutation);
      out.get(AmlPipeline.REVIEW_TAG).output(mutation.toString());

    } catch (Exception e) {
      parentSpan.recordException(e);
      throw e;
    } finally {
      parentSpan.end();
    }
  }
}
