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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link CsvIOStringToCsvRecord}. */
public class CsvIOStringToCsvRecordTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final BadRecordOutput BAD_RECORD_OUTPUT = new BadRecordOutput();

  private static class BadRecordOutput
      extends PTransform<PCollection<BadRecord>, PCollection<BadRecord>> {

    @Override
    public PCollection<BadRecord> expand(PCollection<BadRecord> input) {
      return input.apply(ParDo.of(new badRecordTransformFn()));
    }

    private static class badRecordTransformFn extends DoFn<BadRecord, BadRecord> {
      @DoFn.ProcessElement
      public void process(@Element BadRecord input, OutputReceiver<BadRecord> receiver) {
        System.out.println(input);
        receiver.output(input);
      }
    }
  }

  @Test
  public void testSingleLineCsvRecord() throws IOException {
    String csvRecord = "a,1";
    PCollection<String> input = pipeline.apply(Create.of(csvRecord));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat(), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(Collections.singletonList(Arrays.asList("a", "1")));

    pipeline.run();
  }

  @Test
  public void testMultiLineCsvRecord() throws IOException {
    String csvRecords =
        "\"a\r\n1\",\"a\r\n2\"" + "\n" + "\"b\r\n1\",\"b\r\n2\"" + "\n" + "\"c\r\n1\",\"c\r\n2\"";
    PCollection<String> input = pipeline.apply(Create.of(csvRecords));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withRecordSeparator('\n'), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a\r\n1", "a\r\n2"),
                Arrays.asList("b\r\n1", "b\r\n2"),
                Arrays.asList("c\r\n1", "c\r\n2")));

    pipeline.run();
  }

  @Test
  public void testCsvRecordsWithSkipHeaderRecord() throws IOException {
    String csvRecords = "a_string,an_integer\na,1\nb,2\n";
    PCollection<String> input = pipeline.apply(Create.of(csvRecords));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withSkipHeaderRecord(), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(Arrays.asList(Arrays.asList("a", "1"), Arrays.asList("b", "2")));

    pipeline.run();
  }

  @Test
  public void testCsvRecordsWithCommentMarker() throws IOException {
    String csvRecords = "#leaving a comment\n" + "a,1,1.1\nb,2,2.2\nc,3,3.3";
    PCollection<String> input = pipeline.apply(Create.of(csvRecords));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withCommentMarker('#'), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));

    pipeline.run();
  }

  @Test
  public void testCsvRecordsWithIgnoreEmptyLines() throws IOException {
    String csvRecords = "line1\nline2\nline3\nline4\nline5\n\n\nline6";
    PCollection<String> input = pipeline.apply(Create.of(csvRecords));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withIgnoreEmptyLines(), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(
            Arrays.asList(
                Collections.singletonList("line1"),
                Collections.singletonList("line2"),
                Collections.singletonList("line3"),
                Collections.singletonList("line4"),
                Collections.singletonList("line5"),
                Collections.singletonList("line6")));

    pipeline.run();
  }

  @Test
  public void testCsvRecordWithIgnoreSurroundingSpaces() throws IOException {
    String csvRecord = "    Seattle     ,   WA   ";
    PCollection<String> input = pipeline.apply(Create.of(csvRecord));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withIgnoreSurroundingSpaces(), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(Collections.singletonList(Arrays.asList("Seattle", "WA")));

    pipeline.run();
  }

  @Test
  public void testCsvRecordWithTrailingDelimiter() throws IOException {
    String csvRecord = "a,b,c,";
    PCollection<String> input = pipeline.apply(Create.of(csvRecord));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withTrailingDelimiter(), BAD_RECORD_OUTPUT);
    PAssert.that(input.apply(underTest))
        .containsInAnyOrder(Collections.singletonList(Arrays.asList("a", "b", "c")));

    pipeline.run();
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT.withHeader("a_string", "an_integer", "a_double");
  }
}
