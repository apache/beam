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
package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.schemas.Schema;

/** Utilities for testing schemas. */
public class SchemaTestUtils {
  // Assert that two schemas are equivalent, ignoring field order. This tests that both schemas
  // (recursively) contain the same fields with the same names, but possibly different orders.
  public static void assertSchemaEquivalent(Schema expected, Schema actual) {
    assertTrue(actual.equivalent(expected));
  }
}
