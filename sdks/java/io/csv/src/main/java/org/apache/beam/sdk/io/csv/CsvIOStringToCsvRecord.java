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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * {@link CsvIOStringToCsvRecord} is a class that takes a {@link PCollection<String>} input and
 * outputs a {@link PCollection<CSVRecord>} with potential {@link PCollection<CsvIOParseError>} for
 * targeted error detection.
 */
final class CsvIOStringToCsvRecord
    extends PTransform<PCollection<String>, PCollection<List<String>>> {
  private final CSVFormat csvFormat;

  CsvIOStringToCsvRecord(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  /**
   * Creates {@link PCollection<CSVRecord>} from {@link PCollection<String>} for future processing
   * to Row or custom type.
   */
  @Override
  public PCollection<List<String>> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new ProcessLineToRecordFn()));
  }

  /** Processes each line in order to convert it to a {@link CSVRecord}. */
  private class ProcessLineToRecordFn extends DoFn<String, List<String>> {
    @ProcessElement
    public void process(@Element String line, OutputReceiver<List<String>> receiver)
        throws IOException {
      for (CSVRecord record : CSVParser.parse(line, csvFormat).getRecords()) {
        receiver.output(csvRecordtoList(record));
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
}
