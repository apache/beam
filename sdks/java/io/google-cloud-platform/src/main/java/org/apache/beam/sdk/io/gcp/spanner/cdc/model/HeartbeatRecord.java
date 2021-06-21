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

package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import com.google.cloud.Timestamp;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;

// TODO: Check if we can remove the setters
@DefaultCoder(AvroCoder.class)
public class HeartbeatRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 5331450064150969956L;
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp timestamp;

  /**
   * Default constructor for serialization only.
   */
  private HeartbeatRecord() {}

  public HeartbeatRecord(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartbeatRecord)) {
      return false;
    }
    HeartbeatRecord that = (HeartbeatRecord) o;
    return Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp);
  }

  @Override
  public String toString() {
    return "HeartbeatRecord{" +
        "timestamp=" + timestamp +
        '}';
  }
}
