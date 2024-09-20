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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Instant;

/**
 * {@link CsvIOStringToCsvRecord} is a class that takes a {@link PCollection<String>} input and
 * outputs a {@link PCollection<CSVRecord>} with potential {@link PCollection<CsvIOParseError>} for
 * targeted error detection.
 */
final class CsvIOStringToCsvRecord
    extends PTransform<PCollection<String>, CsvIOParseResult<List<String>>> {

  private final CSVFormat csvFormat;

  private final TupleTag<List<String>> outputTag = new TupleTag<List<String>>() {};

  private final TupleTag<CsvIOParseError> errorTag = new TupleTag<CsvIOParseError>() {};

  CsvIOStringToCsvRecord(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  /**
   * Creates {@link PCollection<CSVRecord>} from {@link PCollection<String>} for future processing
   * to Row or custom type.
   */
  @Override
  public CsvIOParseResult<List<String>> expand(PCollection<String> input) {
    PCollectionTuple pct =
        input.apply(
            ProcessLineToRecordFn.class.getSimpleName(),
            ParDo.of(new ProcessLineToRecordFn())
                .withOutputTags(outputTag, TupleTagList.of(errorTag)));

    return CsvIOParseResult.of(
        outputTag, ListCoder.of(NullableCoder.of(StringUtf8Coder.of())), errorTag, pct);
  }

  /** Processes each line in order to convert it to a {@link CSVRecord}. */
  private class ProcessLineToRecordFn extends DoFn<String, List<String>> {
    private final String headerLine = headerLine(csvFormat);

    @ProcessElement
    public void process(@Element String line, MultiOutputReceiver receiver) {
      if (headerLine.equals(line)) {
        return;
      }
      try (CSVParser csvParser = CSVParser.parse(line, csvFormat)) {
        for (CSVRecord record : csvParser.getRecords()) {
          receiver.get(outputTag).output(csvRecordtoList(record));
        }
      } catch (RuntimeException | IOException e) {
        receiver
            .get(errorTag)
            .output(
                CsvIOParseError.builder()
                    .setCsvRecord(line)
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setObservedTimestamp(Instant.now())
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }
  }

  /** Creates a {@link List<String>} containing {@link CSVRecord} values. */
  private static List<String> csvRecordtoList(CSVRecord record) {
    List<String> cells = new ArrayList<>();
    for (String cell : record) {
      cells.add(cell);
    }
    return cells;
  }

  /** Returns a formatted line of the CSVFormat header. */
  static String headerLine(CSVFormat csvFormat) {
    return String.join(String.valueOf(csvFormat.getDelimiter()), csvFormat.getHeader());
  }
}
