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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;

/** Estimates the logical size of {@link com.google.cloud.spanner.Mutation}. */
class MutationSizeEstimator {

  // Prevent construction.
  private MutationSizeEstimator() {}

  /** Estimates a size of mutation in bytes. */
  static long sizeOf(Mutation m) {
    if (m.getOperation() == Mutation.Op.DELETE) {
      return sizeOf(m.getKeySet());
    }
    long result = 0;
    for (Value v : m.getValues()) {
      switch (v.getType().getCode()) {
        case ARRAY:
          result += estimateArrayValue(v);
          break;
        case STRUCT:
          throw new IllegalArgumentException("Structs are not supported in mutation.");
        default:
          result += estimatePrimitiveValue(v);
      }
    }
    return result;
  }

  private static long sizeOf(KeySet keySet) {
    long result = 0;
    for (Key k : keySet.getKeys()) {
      result += sizeOf(k);
    }
    for (KeyRange kr : keySet.getRanges()) {
      result += sizeOf(kr);
    }
    return result;
  }

  private static long sizeOf(KeyRange kr) {
    return sizeOf(kr.getStart()) + sizeOf(kr.getEnd());
  }

  private static long sizeOf(Key k) {
    long result = 0;
    for (Object part : k.getParts()) {
      if (part == null) {
        continue;
      }
      if (part instanceof Boolean) {
        result += 1;
      } else if (part instanceof Long) {
        result += 8;
      } else if (part instanceof Double) {
        result += 8;
      } else if (part instanceof String) {
        result += ((String) part).length();
      } else if (part instanceof ByteArray) {
        result += ((ByteArray) part).length();
      } else if (part instanceof Timestamp) {
        result += 12;
      } else if (part instanceof Date) {
        result += 12;
      }
    }
    return result;
  }

  /** Estimates a size of the mutation group in bytes. */
  public static long sizeOf(MutationGroup group) {
    long result = 0;
    for (Mutation m : group) {
      result += sizeOf(m);
    }
    return result;
  }

  private static long estimatePrimitiveValue(Value v) {
    switch (v.getType().getCode()) {
      case BOOL:
        return 1;
      case FLOAT32:
        return 4;
      case INT64:
      case FLOAT64:
      case ENUM:
        return 8;
      case DATE:
      case TIMESTAMP:
        return 12;
      case STRING:
      case PG_NUMERIC:
        return v.isNull() ? 0 : v.getString().length();
      case BYTES:
      case PROTO:
        return v.isNull() ? 0 : v.getBytes().length();
      case NUMERIC:
        // see
        // https://cloud.google.com/spanner/docs/working-with-numerics#handling_numeric_when_creating_a_client_library_or_driver
        // Numeric/BigDecimal are stored in protos as String. It is likely that they
        // are also stored in the Spanner database as String, so this gives an approximation for
        // mutation value size.
        return v.isNull() ? 0 : v.getNumeric().toString().length();
      case JSON:
        return v.isNull() ? 0 : v.getJson().length();
      case PG_JSONB:
        return v.isNull() ? 0 : v.getPgJsonb().length();
      default:
        throw new IllegalArgumentException("Unsupported type " + v.getType());
    }
  }

  private static long estimateArrayValue(Value v) {
    if (v.isNull()) {
      return 0;
    }
    switch (v.getType().getArrayElementType().getCode()) {
      case BOOL:
        return v.getBoolArray().size();
      case FLOAT32:
        return 4L * v.getFloat32Array().size();
      case INT64:
      case ENUM:
        return 8L * v.getInt64Array().size();
      case FLOAT64:
        return 8L * v.getFloat64Array().size();
      case STRING:
      case PG_NUMERIC:
        long totalLength = 0;
        for (String s : v.getStringArray()) {
          if (s == null) {
            continue;
          }
          totalLength += s.length();
        }
        return totalLength;
      case BYTES:
      case PROTO:
        totalLength = 0;
        for (ByteArray bytes : v.getBytesArray()) {
          if (bytes == null) {
            continue;
          }
          totalLength += bytes.length();
        }
        return totalLength;
      case DATE:
        return 12L * v.getDateArray().size();
      case TIMESTAMP:
        return 12L * v.getTimestampArray().size();
      case NUMERIC:
        totalLength = 0;
        for (BigDecimal n : v.getNumericArray()) {
          if (n == null) {
            continue;
          }
          // see
          // https://cloud.google.com/spanner/docs/working-with-numerics#handling_numeric_when_creating_a_client_library_or_driver
          // Numeric/BigDecimal are stored in protos as String. It is likely that they
          // are also stored in the Spanner database as String, so this gives an approximation for
          // mutation value size.
          totalLength += n.toString().length();
        }
        return totalLength;
      case JSON:
        totalLength = 0;
        for (String s : v.getJsonArray()) {
          if (s == null) {
            continue;
          }
          totalLength += s.length();
        }
        return totalLength;
      case PG_JSONB:
        totalLength = 0;
        for (String s : v.getPgJsonbArray()) {
          if (s == null) {
            continue;
          }
          totalLength += s.length();
        }
        return totalLength;
      default:
        throw new IllegalArgumentException("Unsupported type " + v.getType());
    }
  }
}
