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
package org.apache.beam.sdk.io.gcp.firestore;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Value.ValueTypeCase;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import com.google.type.LatLng;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/** Helper methods for {@link Firestore}. */
// TODO: Accept OrBuilders when possible.
public final class FirestoreHelper {
  private static final Logger logger = Logger.getLogger(FirestoreHelper.class.getName());

  private static final int MICROSECONDS_PER_SECOND = 1000 * 1000;
  private static final int NANOSECONDS_PER_MICROSECOND = 1000;

  private FirestoreHelper() {}

  /** Make an array value containing the specified values. */
  public static Value.Builder makeValue(Iterable<Value> values) {
    return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(values));
  }

  /** Make a list value containing the specified values. */
  public static Value.Builder makeValue(Value value1, Value value2, Value... rest) {
    ArrayValue.Builder arrayValue = ArrayValue.newBuilder();
    arrayValue.addValues(value1);
    arrayValue.addValues(value2);
    arrayValue.addAllValues(Arrays.asList(rest));
    return Value.newBuilder().setArrayValue(arrayValue);
  }

  /** Make an array value containing the specified values. */
  public static Value.Builder makeValue(
      Value.Builder value1, Value.Builder value2, Value.Builder... rest) {
    ArrayValue.Builder arrayValue = ArrayValue.newBuilder();
    arrayValue.addValues(value1);
    arrayValue.addValues(value2);
    for (Value.Builder builder : rest) {
      arrayValue.addValues(builder);
    }
    return Value.newBuilder().setArrayValue(arrayValue);
  }

  /** Make a reference value. */
  public static Value.Builder makeReferenceValue(String value) {
    return Value.newBuilder().setReferenceValue(value);
  }

  /** Make an integer value. */
  public static Value.Builder makeValue(long value) {
    return Value.newBuilder().setIntegerValue(value);
  }

  /** Make a floating point value. */
  public static Value.Builder makeValue(double value) {
    return Value.newBuilder().setDoubleValue(value);
  }

  /** Make a boolean value. */
  public static Value.Builder makeValue(boolean value) {
    return Value.newBuilder().setBooleanValue(value);
  }

  /** Make a string value. */
  public static Value.Builder makeStringValue(String value) {
    return Value.newBuilder().setStringValue(value);
  }

  /** Make a map value. */
  public static Value.Builder makeValue(MapValue mapValue) {
    return Value.newBuilder().setMapValue(mapValue);
  }

  /** Make a map value. */
  public static Value.Builder makeValue(MapValue.Builder mapValue) {
    return makeValue(mapValue.build());
  }

  /** Make a ByteString value. */
  public static Value.Builder makeValue(ByteString blob) {
    return Value.newBuilder().setBytesValue(blob);
  }

  /** Make a timestamp value given a date. */
  public static Value.Builder makeValue(Date date) {
    return Value.newBuilder().setTimestampValue(toTimestamp(date.getTime() * 1000L));
  }

  private static Timestamp.Builder toTimestamp(long microseconds) {
    long seconds = microseconds / MICROSECONDS_PER_SECOND;
    long microsecondsRemainder = microseconds % MICROSECONDS_PER_SECOND;
    if (microsecondsRemainder < 0) {
      // Nanos must be positive even if microseconds is negative.
      // Java modulus doesn't take care of this for us.
      microsecondsRemainder += MICROSECONDS_PER_SECOND;
      seconds -= 1;
    }
    return Timestamp.newBuilder()
        .setSeconds(seconds)
        .setNanos((int) microsecondsRemainder * NANOSECONDS_PER_MICROSECOND);
  }

  /** Makes a GeoPoint value. */
  public static Value.Builder makeValue(LatLng value) {
    return Value.newBuilder().setGeoPointValue(value);
  }

  /**
   * @return the double contained in value
   * @throws IllegalArgumentException if the value does not contain a double.
   */
  public static double getDouble(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.DOUBLE_VALUE) {
      throw new IllegalArgumentException("Value does not contain a double.");
    }
    return value.getDoubleValue();
  }

  /**
   * @return the reference contained in value
   * @throws IllegalArgumentException if the value does not contain a reference.
   */
  public static String getReference(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.REFERENCE_VALUE) {
      throw new IllegalArgumentException("Value does not contain a reference.");
    }
    return value.getReferenceValue();
  }

  /**
   * @return the blob contained in value
   * @throws IllegalArgumentException if the value does not contain a blob.
   */
  public static ByteString getByteString(Value value) {
    if (value.getValueTypeCase() == ValueTypeCase.BYTES_VALUE) {
      return value.getBytesValue();
    }
    throw new IllegalArgumentException("Value does not contain a blob.");
  }

  /**
   * @return the map contained in value
   * @throws IllegalArgumentException if the value does not contain a map.
   */
  public static MapValue getMap(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.MAP_VALUE) {
      throw new IllegalArgumentException("Value does not contain a Map.");
    }
    return value.getMapValue();
  }

  /**
   * @return the string contained in value
   * @throws IllegalArgumentException if the value does not contain a string.
   */
  public static String getString(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.STRING_VALUE) {
      throw new IllegalArgumentException("Value does not contain a string.");
    }
    return value.getStringValue();
  }

  /**
   * @return the boolean contained in value
   * @throws IllegalArgumentException if the value does not contain a boolean.
   */
  public static boolean getBoolean(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.BOOLEAN_VALUE) {
      throw new IllegalArgumentException("Value does not contain a boolean.");
    }
    return value.getBooleanValue();
  }

  /**
   * @return the long contained in value
   * @throws IllegalArgumentException if the value does not contain a long.
   */
  public static long getLong(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.INTEGER_VALUE) {
      throw new IllegalArgumentException("Value does not contain an integer.");
    }
    return value.getIntegerValue();
  }

  /**
   * @return the timestamp in microseconds contained in value
   * @throws IllegalArgumentException if the value does not contain a timestamp.
   */
  public static long getTimestamp(Value value) {
    if (value.getValueTypeCase() == ValueTypeCase.TIMESTAMP_VALUE) {
      return toMicroseconds(value.getTimestampValue());
    }
    throw new IllegalArgumentException("Value does not contain a timestamp.");
  }

  private static long toMicroseconds(TimestampOrBuilder timestamp) {
    // Nanosecond precision is lost.
    return timestamp.getSeconds() * MICROSECONDS_PER_SECOND
        + timestamp.getNanos() / NANOSECONDS_PER_MICROSECOND;
  }

  /**
   * @return the array contained in value as a list.
   * @throws IllegalArgumentException if the value does not contain an array.
   */
  public static List<Value> getList(Value value) {
    if (value.getValueTypeCase() != ValueTypeCase.ARRAY_VALUE) {
      throw new IllegalArgumentException("Value does not contain an array.");
    }
    return value.getArrayValue().getValuesList();
  }

  /**
   * Convert a timestamp value into a {@link Date} clipping off the microseconds.
   *
   * @param value a timestamp value to convert
   * @return the resulting {@link Date}
   * @throws IllegalArgumentException if the value does not contain a timestamp.
   */
  public static Date toDate(Value value) {
    return new Date(getTimestamp(value) / 1000);
  }
}
