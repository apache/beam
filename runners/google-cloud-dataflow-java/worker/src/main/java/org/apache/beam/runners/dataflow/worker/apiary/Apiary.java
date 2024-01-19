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
package org.apache.beam.runners.dataflow.worker.apiary;

import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * Static convenience methods to work around default encodings done by Apiary for default fields.
 */
public class Apiary {
  /**
   * Returns an empty list if list is null otherwise returns the list. Apiary encodes empty lists as
   * null. This function simplifies handling list fields by treating nulls as empty lists.
   */
  public static <V> List<V> listOrEmpty(List<V> list) {
    return list == null ? ImmutableList.<V>of() : list;
  }

  /**
   * Apiary fields with default values returns null instead of 0 for integer types. This function
   * simplifies handling {@code int} fields by returning the default of 0.
   */
  public static int intOrZero(Integer value) {
    if (value == null) {
      return 0;
    }
    return value;
  }
}
