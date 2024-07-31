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

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CsvIOTest {
  private static final String[] HEADER =
      new String[] {"aBoolean", "aDouble", "aFloat", "anInteger", "aLong", "aString"};

  @Test
  public void givenIncorrectCellValueForCustomType_throws() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        csvRecords(
            pipeline,
            "# This is a comment",
            "aBoolean,aDouble,aFloat,anInteger,aLong,aString",
            "true,1.0,2.0,3.51234,4,foo");
    CsvIOParse<SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes> underTest =
        CsvIO.parse(SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes.class, csvFormat());
    CsvIOParseResult<SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes> result =
        input.apply(underTest);
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);
    pipeline.run();
  }

  @Test
  public void parseRowsTest() {
    Pipeline pipeline = Pipeline.create();

    pipeline.run();
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader(HEADER)
        .withCommentMarker('#')
        .withNullString("🏵")
        .withEscape('$');
  }

  private static PCollection<String> csvRecords(Pipeline pipeline, String... lines) {
    return pipeline.apply(
        Create.of(Arrays.asList(lines)).withCoder(NullableCoder.of(StringUtf8Coder.of())));
  }
}
