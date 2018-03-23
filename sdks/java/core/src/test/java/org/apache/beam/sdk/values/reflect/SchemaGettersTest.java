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

package org.apache.beam.sdk.values.reflect;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertSame;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

/**
 * Unit tests for {@link RowTypeGetters}.
 */
public class SchemaGettersTest {

  @Test
  public void testGetters() {
    Schema schema = Schema.of(emptyList());
    List<FieldValueGetter> fieldValueGetters = emptyList();

    RowTypeGetters getters = new RowTypeGetters(schema, fieldValueGetters);

    assertSame(schema, getters.rowType());
    assertSame(fieldValueGetters, getters.valueGetters());
  }
}
