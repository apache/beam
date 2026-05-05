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
package org.apache.beam.sdk.values;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;

/** Utility class for converting between {@link ValueKind} and {@link Elements.ValueKind.Enum}. */
public class ValueKindUtil {
  public static Elements.ValueKind.Enum toProto(ValueKind valueKind) {
    switch (valueKind) {
      case INSERT:
        return Elements.ValueKind.Enum.INSERT;
      case UPDATE_BEFORE:
        return Elements.ValueKind.Enum.UPDATE_BEFORE;
      case UPDATE_AFTER:
        return Elements.ValueKind.Enum.UPDATE_AFTER;
      case DELETE:
        return Elements.ValueKind.Enum.DELETE;
      default:
        throw new IllegalArgumentException("Unknown ValueKind: " + valueKind);
    }
  }

  public static ValueKind fromProto(Elements.ValueKind.Enum proto) {
    switch (proto) {
      case VALUE_KIND_UNSPECIFIED:
      case INSERT:
        return ValueKind.INSERT;
      case UPDATE_BEFORE:
        return ValueKind.UPDATE_BEFORE;
      case UPDATE_AFTER:
        return ValueKind.UPDATE_AFTER;
      case DELETE:
        return ValueKind.DELETE;
      default:
        throw new IllegalArgumentException("Unknown ValueKind: " + proto);
    }
  }
}
