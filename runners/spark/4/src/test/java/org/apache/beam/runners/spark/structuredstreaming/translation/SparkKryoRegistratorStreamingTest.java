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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.execution.streaming.sources.MemoryWriterCommitMessage;
import org.apache.spark.sql.execution.streaming.state.StateSchemaMetadata;
import org.apache.spark.sql.execution.streaming.state.StateSchemaMetadataKey;
import org.apache.spark.sql.execution.streaming.state.StateSchemaMetadataValue;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.collection.immutable.Map$;

/**
 * Guards the Spark 4 streaming entries of {@link SparkSessionFactory.SparkKryoRegistrator}. The
 * registrator references them by name, so a rename in a future Spark version would silently drop
 * the registration and only surface as a streaming query dying on its first micro-batch. This test
 * names the classes at compile time, turning that failure mode into a compile error.
 *
 * @see SparkSessionFactory.SparkKryoRegistrator
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class SparkKryoRegistratorStreamingTest {

  /** A Kryo configured exactly the way the runner configures it, strict registration included. */
  private static Kryo strictKryo() {
    SparkConf conf =
        new SparkConf(false)
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.kryo.registrationRequired", "true")
            .set(
                "spark.kryo.registrator", SparkSessionFactory.SparkKryoRegistrator.class.getName());
    return new KryoSerializer(conf).newKryo();
  }

  private static Object roundTrip(Kryo kryo, Object value) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (Output output = new Output(bytes)) {
      kryo.writeClassAndObject(output, value);
    }
    try (Input input = new Input(new ByteArrayInputStream(bytes.toByteArray()))) {
      return kryo.readClassAndObject(input);
    }
  }

  /**
   * Spark 4 broadcasts a {@link StateSchemaMetadata} to the executors for every {@code
   * transformWithState} query, so this is the registration that decides whether a Beam streaming
   * pipeline with state or timers runs at all under {@code spark.kryo.registrationRequired=true}.
   *
   * <p>The instance below is deliberately not empty. It carries the nested {@code StructType} and
   * {@code org.apache.avro.Schema} that make the difference between a registration that only
   * survives a trivial payload and one that survives a real one.
   */
  @Test
  public void stateSchemaMetadataRoundTripsWithRegistrationRequired() {
    StructType sqlSchema =
        new StructType().add("key", DataTypes.StringType).add("value", DataTypes.BinaryType, false);
    StateSchemaMetadataKey key = new StateSchemaMetadataKey("default", (short) 1, true);
    StateSchemaMetadataValue value =
        new StateSchemaMetadataValue(
            sqlSchema, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    StateSchemaMetadata metadata =
        new StateSchemaMetadata(Map$.MODULE$.from(seqOf(tuple(key, value))));

    Kryo kryo = strictKryo();
    assertNotNull(
        "StateSchemaMetadata must be registered, see SparkKryoRegistrator",
        kryo.getRegistration(StateSchemaMetadata.class));

    StateSchemaMetadata back = (StateSchemaMetadata) roundTrip(kryo, metadata);
    assertEquals(1, back.activeSchemas().size());
    assertEquals(value, back.activeSchemas().apply(key));
  }

  /**
   * The commit message of Spark's {@code memory} sink, nested inside the already registered {@code
   * DataWritingSparkTaskResult}. The runner writes to {@code noop}, but the {@code memory} sink is
   * the obvious thing to reach for when inspecting a query, and it used to fail on batch 0.
   */
  @Test
  public void memoryWriterCommitMessageRoundTripsWithRegistrationRequired() {
    Row row = RowFactory.create("a", 1);
    MemoryWriterCommitMessage message = new MemoryWriterCommitMessage(3, seqOf(row));

    Kryo kryo = strictKryo();
    assertNotNull(
        "MemoryWriterCommitMessage must be registered, see SparkKryoRegistrator",
        kryo.getRegistration(MemoryWriterCommitMessage.class));

    MemoryWriterCommitMessage back = (MemoryWriterCommitMessage) roundTrip(kryo, message);
    assertEquals(3, back.partition());
    assertEquals(1, back.data().size());
    assertEquals(row, back.data().apply(0));
  }
}
