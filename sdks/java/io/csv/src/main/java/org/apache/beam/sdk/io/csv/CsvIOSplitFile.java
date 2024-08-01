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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvIOSplitFile extends PTransform<PCollection<ReadableFile>, PCollectionTuple> {
  private final CSVFormat csvFormat;
  final TupleTag<KV<String, List<String>>> outputTag = new TupleTag<KV<String, List<String>>>() {};
  final TupleTag<BadRecord> errorTag = new TupleTag<BadRecord>() {};
  private final Coder<KV<String, List<String>>> coder =
      KvCoder.of(StringUtf8Coder.of(), ListCoder.of(NullableCoder.of(StringUtf8Coder.of())));

  public CsvIOSplitFile(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollectionTuple expand(PCollection<ReadableFile> input) {
    return input.apply(
        ParDo.of(new SplitFileFn()).withOutputTags(outputTag, TupleTagList.of(errorTag)));
  }

  private class SplitFileFn extends DoFn<ReadableFile, KV<String, List<String>>> {
    private List<String> recordValues = new ArrayList<>();

    @ProcessElement
    public void process(@Element FileIO.ReadableFile file, MultiOutputReceiver receiver)
        throws IOException {
      String filename = checkStateNotNull(file.getMetadata().resourceId().getFilename());
      try (CSVParser csvParser = CSVParser.parse(file.readFullyAsUTF8String(), csvFormat)) {
        for (CSVRecord record : csvParser.getRecords()) {
          recordValues = CsvIOStringToCsvRecord.csvRecordtoList(record);
          receiver
              .get(outputTag)
              .output(KV.of(filename, CsvIOStringToCsvRecord.csvRecordtoList(record)));
        }
      } catch (RuntimeException e) {
        receiver
            .get(errorTag)
            .output(
                BadRecord.builder()
                    .setFailure(
                        BadRecord.Failure.builder()
                            .setDescription("")
                            .setException(e.getMessage())
                            .build())
                    .setRecord(
                        BadRecord.Record.builder()
                            .addCoderAndEncodedRecord(coder, KV.of(filename, recordValues))
                            .build())
                    .build());
      }
    }
  }
}
