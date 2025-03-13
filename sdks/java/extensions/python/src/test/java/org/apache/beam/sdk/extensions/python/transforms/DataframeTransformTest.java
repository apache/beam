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
package org.apache.beam.sdk.extensions.python.transforms;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.BaseExternalTest;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataframeTransformTest extends BaseExternalTest {
  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testDataframeSum() {
    Schema schema =
        Schema.of(
            Schema.Field.of("a", Schema.FieldType.INT64),
            Schema.Field.of("b", Schema.FieldType.INT32));
    Row foo1 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 1).build();
    Row foo2 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 2).build();
    Row foo3 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 3).build();
    Row bar4 = Row.withSchema(schema).withFieldValue("a", 200L).withFieldValue("b", 4).build();
    PCollection<Row> col =
        testPipeline
            .apply(Create.of(foo1, foo2, bar4))
            .setRowSchema(schema)
            .apply(
                DataframeTransform.of("lambda df: df.groupby('a').sum()")
                    .withIndexes()
                    .withExpansionService(expansionAddr));
    PAssert.that(col).containsInAnyOrder(foo3, bar4);
  }
}
