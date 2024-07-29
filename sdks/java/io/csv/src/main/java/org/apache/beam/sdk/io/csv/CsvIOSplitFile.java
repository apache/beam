package org.apache.beam.sdk.io.csv;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.beam.sdk.transforms.errorhandling.BadRecord.Failure;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CsvIOSplitFile extends PTransform<PCollection<ReadableFile>, PCollection<KV<String, List<String>>>> {
  private final CSVFormat csvFormat;
  public CsvIOSplitFile(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<KV<String, List<String>>> expand(PCollection<ReadableFile> input) {
    return input.apply(ParDo.of(new SplitFileFn())).setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(
        NullableCoder.of(StringUtf8Coder.of()))));
  }

  private class SplitFileFn extends DoFn<ReadableFile, KV<String, List<String>>>{
    @ProcessElement
    public void process(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, List<String>>> receiver)
        throws IOException {
      String filename = checkStateNotNull(file.getMetadata().resourceId().getFilename());
      try (CSVParser csvParser = CSVParser.parse(file.readFullyAsUTF8String(), csvFormat)) {
        for(CSVRecord record : csvParser.getRecords()) {
          receiver.output(KV.of(filename, CsvIOStringToCsvRecord.csvRecordtoList(record)));
        }
      }
    }
  }

}
