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
package org.apache.beam.sdk.io.csv;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class CsvIOParseErrorTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final SchemaProvider SCHEMA_PROVIDER = new DefaultSchema.DefaultSchemaProvider();

  @Test
  public void usableInSingleOutput() {
    List<CsvIOParseError> want =
        Arrays.asList(
            CsvIOParseError.builder()
                .setMessage("error message")
                .setObservedTimestamp(Instant.now())
                .setStackTrace("stack trace")
                .build(),
            CsvIOParseError.builder()
                .setMessage("error message")
                .setObservedTimestamp(Instant.now())
                .setStackTrace("stack trace")
                .setFilename("filename")
                .setCsvRecord("csv record")
                .build());

    PCollection<CsvIOParseError> errors = pipeline.apply(Create.of(want));
    PAssert.that(errors).containsInAnyOrder(want);

    pipeline.run();
  }

  @Test
  public void usableInMultiOutput() {
    List<CsvIOParseError> want =
        Arrays.asList(
            CsvIOParseError.builder()
                .setMessage("error message")
                .setObservedTimestamp(Instant.now())
                .setStackTrace("stack trace")
                .build(),
            CsvIOParseError.builder()
                .setMessage("error message")
                .setObservedTimestamp(Instant.now())
                .setStackTrace("stack trace")
                .setFilename("filename")
                .setCsvRecord("csv record")
                .build());

    TupleTag<CsvIOParseError> errorTag = new TupleTag<CsvIOParseError>() {};
    TupleTag<String> anotherTag = new TupleTag<String>() {};

    PCollection<CsvIOParseError> errors = pipeline.apply("createWant", Create.of(want));
    PCollection<String> anotherPCol = pipeline.apply("createAnother", Create.of("a", "b", "c"));
    PCollectionTuple pct = PCollectionTuple.of(errorTag, errors).and(anotherTag, anotherPCol);
    PAssert.that(pct.get(errorTag)).containsInAnyOrder(want);

    pipeline.run();
  }

  @Test
  public void canDeriveSchema() {
    TypeDescriptor<CsvIOParseError> type = TypeDescriptor.of(CsvIOParseError.class);
    Schema schema = SCHEMA_PROVIDER.schemaFor(type);
    assertNotNull(schema);
    pipeline.run();
  }
}
