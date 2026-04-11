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
package org.apache.beam.sdk.io.arrowflight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.ServerHeaderMiddleware;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ArrowFlightIO}. */
@RunWith(JUnit4.class)
public class ArrowFlightIOTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private BufferAllocator allocator;
  private FlightServer server;
  private TestFlightProducer producer;
  private int port;

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    producer = new TestFlightProducer(allocator);

    // Bind to any available port
    Location location = Location.forGrpcInsecure("localhost", 0);
    server = FlightServer.builder(allocator, location, producer).build();
    server.start();

    port = server.getPort();
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testRead() {
    PCollection<Row> output =
        pipeline.apply(
            "Read from Flight",
            ArrowFlightIO.read().withHost("localhost").withPort(port).withCommand("test_query"));

    Schema expectedSchema = Schema.builder().addStringField("name").build();
    Row expectedRow1 = Row.withSchema(expectedSchema).addValue("Alice").build();
    Row expectedRow2 = Row.withSchema(expectedSchema).addValue("Bob").build();

    PAssert.that(output).containsInAnyOrder(expectedRow1, expectedRow2);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWrite() throws Exception {
    Schema expectedSchema = Schema.builder().addStringField("name").build();
    Row row1 = Row.withSchema(expectedSchema).addValue("Charlie").build();
    Row row2 = Row.withSchema(expectedSchema).addValue("Dave").build();

    pipeline
        .apply(Create.of(row1, row2).withRowSchema(expectedSchema))
        .apply(
            "Write to Flight",
            ArrowFlightIO.write()
                .withHost("localhost")
                .withPort(port)
                .withDescriptor("test_table"));

    pipeline.run().waitUntilFinish();

    assertEquals(2, producer.writtenRecords.get());
  }

  @Test
  public void testWriteWithToken() throws Exception {
    producer.requiredAuthorizationHeader = "Bearer test-token";

    Schema expectedSchema = Schema.builder().addStringField("name").build();
    Row row = Row.withSchema(expectedSchema).addValue("Charlie").build();

    pipeline
        .apply(Create.of(row).withRowSchema(expectedSchema))
        .apply(
            "Write to Flight with Token",
            ArrowFlightIO.write()
                .withHost("localhost")
                .withPort(port)
                .withDescriptor("test_table")
                .withToken("test-token".getBytes(StandardCharsets.UTF_8)));

    pipeline.run().waitUntilFinish();

    assertEquals("Bearer test-token", producer.lastAuthorizationHeader.get());
    assertEquals(1, producer.writtenRecords.get());
  }

  @Test
  public void testWritePropagatesServerErrors() {
    producer.failWrites = true;

    Schema expectedSchema = Schema.builder().addStringField("name").build();
    Row row = Row.withSchema(expectedSchema).addValue("Charlie").build();

    pipeline
        .apply(Create.of(row).withRowSchema(expectedSchema))
        .apply(
            "Write to Failing Flight Server",
            ArrowFlightIO.write()
                .withHost("localhost")
                .withPort(port)
                .withDescriptor("test_table"));

    PipelineExecutionException exception =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
    assertThat(exception.getMessage(), containsString("Rejected write"));
  }

  @Test
  public void testWriteRejectsUnsupportedSchema() {
    Schema unsupportedSchema =
        Schema.builder().addArrayField("names", Schema.FieldType.STRING).build();
    Row row =
        Row.withSchema(unsupportedSchema).addArray(Collections.singletonList("Charlie")).build();
    Pipeline testPipeline = Pipeline.create();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                testPipeline
                    .apply(Create.of(row).withRowSchema(unsupportedSchema))
                    .apply(
                        ArrowFlightIO.write()
                            .withHost("localhost")
                            .withPort(port)
                            .withDescriptor("test_table")));

    assertThat(exception.getMessage(), containsString("does not support Beam type 'ARRAY'"));
  }

  /** A simple FlightProducer that returns predefined data for reads and counts writes. */
  private static class TestFlightProducer implements FlightProducer {

    private final BufferAllocator allocator;
    final AtomicInteger writtenRecords = new AtomicInteger();
    final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();
    volatile boolean failWrites;
    volatile String requiredAuthorizationHeader;

    TestFlightProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(
              Collections.singletonList(
                  new Field(
                      "name", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList())));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        listener.start(root);

        VarCharVector vector = (VarCharVector) root.getVector("name");
        vector.allocateNew();
        vector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        vector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        vector.setValueCount(2);
        root.setRowCount(2);

        listener.putNext();
        listener.completed();
      } catch (Exception e) {
        listener.error(e);
      }
    }

    @Override
    public void listFlights(
        CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
      listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(
              Collections.singletonList(
                  new Field(
                      "name", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList())));
      return new FlightInfo(
          schema,
          descriptor,
          Collections.singletonList(
              new FlightEndpoint(
                  new Ticket(descriptor.getCommand()), Location.forGrpcInsecure("localhost", 0))),
          -1,
          -1);
    }

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      ServerHeaderMiddleware headerMiddleware = context.getMiddleware(FlightConstants.HEADER_KEY);
      lastAuthorizationHeader.set(
          headerMiddleware == null ? null : headerMiddleware.headers().get("authorization"));

      return () -> {
        try {
          if (requiredAuthorizationHeader != null
              && !requiredAuthorizationHeader.equals(lastAuthorizationHeader.get())) {
            ackStream.onError(
                CallStatus.UNAUTHENTICATED
                    .withDescription("Missing or invalid authorization header")
                    .toRuntimeException());
            return;
          }
          while (flightStream.next()) {
            VectorSchemaRoot root = flightStream.getRoot();
            writtenRecords.addAndGet(root.getRowCount());
          }
          if (failWrites) {
            ackStream.onError(
                CallStatus.INTERNAL.withDescription("Rejected write").toRuntimeException());
            return;
          }
          ackStream.onCompleted();
        } catch (Exception e) {
          ackStream.onError(e);
        }
      };
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
      listener.onCompleted();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
      listener.onCompleted();
    }
  }
}
