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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder;

import com.google.cloud.Timestamp;
import java.io.IOException;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;

/**
 * This encoder/decoder writes a com.google.cloud.Timestamp object as a pair of long and int to avro
 * and reads a Timestamp object from the same pair. The long stores the number of seconds since
 * January 1, 1970, 00:00:00 UTC. A negative value is the number of seconds before January 1, 1970,
 * 00:00:00 UTC. The int stores the fractional seconds components, in the range 0..999999999.
 */
public class TimestampEncoding extends CustomEncoding<Timestamp> {

  {
    /*
     * The schema of a Avro Encoded timestamp. It is represented as a record with a {@link Long}
     * seconds part and a {@link Integer} nanoseconds part.
     */
    this.schema =
        SchemaBuilder.builder()
            .record("timestamp")
            .fields()
            .requiredLong("seconds")
            .requiredInt("nanos")
            .endRecord();
    this.schema.addProp("CustomEncoding", TimestampEncoding.class.getSimpleName());
  }

  /**
   * Serializes a {@link Timestamp} received as datum to the output encoder out. A null timestamp is
   * serialized with negative fields (seconds and nanos).
   *
   * @param datum the {@link Timestamp} to be encoded
   * @param out the {@link Encoder} where the timestamp should be serialized to
   * @throws IOException if it was not possible to write the timestamp into the provided encoder
   */
  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    final Timestamp timestamp = (Timestamp) datum;
    if (timestamp == null) {
      out.writeLong(-1L);
      out.writeInt(-1);
    } else {
      out.writeLong(timestamp.getSeconds());
      out.writeInt(timestamp.getNanos());
    }
  }

  /**
   * Deserializes a {@link Timestamp} from the given {@link Decoder}.
   *
   * @param reuse ignored
   * @param in the {@link Decoder} to read the timestamp fields from
   * @return null if the fields are negative or an instance of a {@link Timestamp} instead.
   * @throws IOException if it was not possible to read the timestamp from the provided decoder
   */
  // it is possible to return nulls here if the encoded value was null
  @SuppressWarnings({"override.return", "return"})
  @Override
  protected Timestamp read(Object reuse, Decoder in) throws IOException {
    final long seconds = in.readLong();
    final int nanos = in.readInt();
    if (seconds < 0 && nanos < 0) {
      return null;
    } else {
      return Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
    }
  }
}
