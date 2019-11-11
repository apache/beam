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
package org.apache.beam.sdk.io.thrift.parser.util;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnumField;

public class IntegerEnumFieldList extends ArrayList<IntegerEnumField> implements Serializable {
  public long getNextImplicitEnumerationValue() {
    int lastFieldIndex = size() - 1;
    if (lastFieldIndex < 0) {
      // No previous values to derive from, and thrift enumerations start at zero by default
      return 0;
    }
    IntegerEnumField lastField = get(lastFieldIndex);
    return lastField.getValue() + 1;
  }
}
