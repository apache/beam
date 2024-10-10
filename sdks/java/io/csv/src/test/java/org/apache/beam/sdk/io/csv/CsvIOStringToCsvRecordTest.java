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

import static org.apache.beam.sdk.io.csv.CsvIOStringToCsvRecord.headerLine;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvIOStringToCsvRecord}. */
@RunWith(JUnit4.class)
public class CsvIOStringToCsvRecordTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final String[] header = {"a_string", "an_integer", "a_double"};

  @Test
  public void givenCommentMarker_skipsLine() {
    CSVFormat csvFormat = csvFormat().withCommentMarker('#');
    PCollection<String> input =
        pipeline.apply(
            Create.of(headerLine(csvFormat), "#should skip me", "a,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNoCommentMarker_doesntSkipLine() {
    CSVFormat csvFormat = csvFormat();
    PCollection<String> input =
        pipeline.apply(
            Create.of(headerLine(csvFormat), "#comment", "a,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Collections.singletonList("#comment"),
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenCustomDelimiter_splitsCells() {
    CSVFormat csvFormat = csvFormat().withDelimiter(';');
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a;1;1.1", "b;2;2.2", "c;3;3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenEscapeCharacter_includeInCell() {
    CSVFormat csvFormat = csvFormat().withEscape('$');
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a$,b,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a,b", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenHeaderComment_isNoop() {
    CSVFormat csvFormat = csvFormat().withHeaderComments("abc", "def", "xyz");
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenIgnoreEmptyLines_shouldSkip() {
    CSVFormat csvFormat = csvFormat().withIgnoreEmptyLines(true);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1", "", "b,2,2.2", "", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNoIgnoreEmptyLines_isNoop() {
    CSVFormat csvFormat = csvFormat().withIgnoreEmptyLines(false);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1", "", "b,2,2.2", "", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenIgnoreSurroundingSpaces_removesSpaces() {
    CSVFormat csvFormat = csvFormat().withIgnoreSurroundingSpaces(true);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "  a  ,1,1.1",
                "b,        2     ,2.2",
                "c,3,   3.3         "));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNotIgnoreSurroundingSpaces_keepsSpaces() {
    CSVFormat csvFormat = csvFormat().withIgnoreSurroundingSpaces(false);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "  a  ,1,1.1",
                "b,        2     ,2.2",
                "c,3,   3.3         "));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("  a  ", "1", "1.1"),
                Arrays.asList("b", "        2     ", "2.2"),
                Arrays.asList("c", "3", "   3.3         ")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNullString_parsesNullCells() {
    CSVFormat csvFormat = csvFormat().withNullString("🐼");
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,🐼", "b,🐼,2.2", "🐼,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", null),
                Arrays.asList("b", null, "2.2"),
                Arrays.asList(null, "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNoNullString_isNoop() {
    CSVFormat csvFormat = csvFormat();
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,🐼", "b,🐼,2.2", "🐼,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "🐼"),
                Arrays.asList("b", "🐼", "2.2"),
                Arrays.asList("🐼", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenCustomQuoteCharacter_includesSpecialCharacters() {
    CSVFormat csvFormat = csvFormat().withQuote(':');
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), ":a,:,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a,", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenQuoteModeAll_isNoop() {
    CSVFormat csvFormat = csvFormat().withQuoteMode(QuoteMode.ALL);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "\"a\",\"1\",\"1.1\"",
                "\"b\",\"2\",\"2.2\"",
                "\"c\",\"3\",\"3.3\""));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenQuoteModeAllNonNull_isNoop() {
    CSVFormat csvFormat = csvFormat().withNullString("N/A").withQuoteMode(QuoteMode.ALL_NON_NULL);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "\"a\",\"1\",N/A",
                "\"b\",\"2\",\"2.2\"",
                "\"c\",\"3\",\"3.3\""));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", null),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenQuoteModeMinimal_isNoop() {
    CSVFormat csvFormat = csvFormat().withQuoteMode(QuoteMode.MINIMAL);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "\"a,\",1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a,", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenQuoteModeNonNumeric_isNoop() {
    CSVFormat csvFormat = csvFormat().withQuoteMode(QuoteMode.NON_NUMERIC);
    PCollection<String> input =
        pipeline.apply(
            Create.of(headerLine(csvFormat), "\"a\",1,1.1", "\"b\",2,2.2", "\"c\",3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenQuoteModeNone_isNoop() {
    CSVFormat csvFormat = csvFormat().withEscape('$').withQuoteMode(QuoteMode.NONE);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1", "b,2,2.2", "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenCustomRecordSeparator_isNoop() {
    CSVFormat csvFormat = csvFormat().withRecordSeparator("😆");
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1😆b,2,2.2😆c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Collections.singletonList(
                Arrays.asList("a", "1", "1.1😆b", "2", "2.2😆c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenSystemRecordSeparator_isNoop() {
    CSVFormat csvFormat = csvFormat().withSystemRecordSeparator();
    String systemRecordSeparator = csvFormat.getRecordSeparator();
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "a,1,1.1" + systemRecordSeparator + "b,2,2.2" + systemRecordSeparator + "c,3,3.3"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenTrailingDelimiter_skipsEndingDelimiter() {
    CSVFormat csvFormat = csvFormat().withTrailingDelimiter(true);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1,", "b,2,2.2,", "c,3,3.3,"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNoTrailingDelimiter_includesEndingCell() {
    CSVFormat csvFormat = csvFormat().withTrailingDelimiter(false);
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,1,1.1,", "b,2,2.2,", "c,3,3.3,"));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1", ""),
                Arrays.asList("b", "2", "2.2", ""),
                Arrays.asList("c", "3", "3.3", "")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenTrim_removesSpaces() {
    CSVFormat csvFormat = csvFormat().withTrim(true);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "  a  ,1,1.1",
                "b,        2     ,2.2",
                "c,3,   3.3         "));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a", "1", "1.1"),
                Arrays.asList("b", "2", "2.2"),
                Arrays.asList("c", "3", "3.3")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenNoTrim_keepsSpaces() {
    CSVFormat csvFormat = csvFormat().withTrim(false);
    PCollection<String> input =
        pipeline.apply(
            Create.of(
                headerLine(csvFormat),
                "  a  ,1,1.1",
                "b,        2     ,2.2",
                "c,3,   3.3         "));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("  a  ", "1", "1.1"),
                Arrays.asList("b", "        2     ", "2.2"),
                Arrays.asList("c", "3", "   3.3         ")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void testSingleLineCsvRecord() {
    String csvRecord = "a,1";
    PCollection<String> input = pipeline.apply(Create.of(csvRecord));

    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat());
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(Collections.singletonList(Arrays.asList("a", "1")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void testMultiLineCsvRecord() {
    String csvRecords =
        "\"a\r\n1\",\"a\r\n2\"" + "\n" + "\"b\r\n1\",\"b\r\n2\"" + "\n" + "\"c\r\n1\",\"c\r\n2\"";
    PCollection<String> input = pipeline.apply(Create.of(csvRecords));

    CsvIOStringToCsvRecord underTest =
        new CsvIOStringToCsvRecord(csvFormat().withRecordSeparator('\n'));
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.that(result.getOutput())
        .containsInAnyOrder(
            Arrays.asList(
                Arrays.asList("a\r\n1", "a\r\n2"),
                Arrays.asList("b\r\n1", "b\r\n2"),
                Arrays.asList("c\r\n1", "c\r\n2")));
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenInvalidCsvRecord_throws() {
    CSVFormat csvFormat = csvFormat().withQuote('"');
    PCollection<String> input =
        pipeline.apply(Create.of(headerLine(csvFormat), "a,\"1,1.1", "b,2,2.2", "c,3,3.3"));
    CsvIOStringToCsvRecord underTest = new CsvIOStringToCsvRecord(csvFormat);
    CsvIOParseResult<List<String>> result = input.apply(underTest);
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);
    pipeline.run();
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT.withAllowDuplicateHeaderNames(false).withHeader(header);
  }
}
