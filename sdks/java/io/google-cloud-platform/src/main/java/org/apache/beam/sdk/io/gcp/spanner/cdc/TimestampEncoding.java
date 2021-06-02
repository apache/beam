/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.spanner.cdc;

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
    this.schema = SchemaBuilder
        .builder()
        .record("timestamp")
        .fields()
        .requiredLong("seconds")
        .requiredInt("nanos")
        .endRecord();
    this.schema.addProp("CustomEncoding", TimestampEncoding.class.getSimpleName());
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    final Timestamp timestamp = (Timestamp) datum;
    out.writeLong(timestamp.getSeconds());
    out.writeInt(timestamp.getNanos());
  }

  @Override
  protected Timestamp read(Object reuse, Decoder in) throws IOException {
    return Timestamp.ofTimeSecondsAndNanos(
        in.readLong(),
        in.readInt()
    );
  }
}
