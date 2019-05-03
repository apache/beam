package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.extensions.smb.json.JsonFileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SmbBenchmark {

  public interface SourceOptions extends PipelineOptions {
    String getAvroSource();
    void setAvroSource(String value);

    String getJsonSource();
    void setJsonSource(String value);
  }

  public static void main(String[] args) throws IOException {
    SourceOptions sourceOptions = PipelineOptionsFactory.fromArgs(args).as(SourceOptions.class);
    Pipeline pipeline = Pipeline.create(sourceOptions);

    SortedBucketSource<String, KV<Iterable<GenericRecord>, Iterable<TableRow>>> source = SortedBucketIO.SortedBucketSourceJoinBuilder
        .withFinalKeyType(String.class)
        .of(FileSystems.matchNewResource(sourceOptions.getAvroSource(), true),
            ".avro",
            new AvroFileOperations<>(null, AvroGeneratedUser.getClassSchema()),
            AvroCoder.of(AvroGeneratedUser.getClassSchema()))
        .and(FileSystems.matchNewResource(sourceOptions.getJsonSource(), true),
            ".json",
            new JsonFileOperations(),
            TableRowJsonCoder.of())
        .build();

    pipeline
        .apply(source)
        .apply(FlatMapElements
        .into(TypeDescriptors.kvs(
            TypeDescriptors.strings(),
            TypeDescriptors.kvs(
                TypeDescriptor.of(GenericRecord.class),
                TypeDescriptor.of(TableRow.class))))
        .via(kv -> {
          String key = kv.getKey();
          Iterable<GenericRecord> il = kv.getValue().getKey();
          Iterable<TableRow> ir = kv.getValue().getValue();
          List<KV<String, KV<GenericRecord, TableRow>>> output = new ArrayList<>();
          for (GenericRecord l : il) {
            for (TableRow r : ir) {
              output.add(KV.of(key, KV.of(l, r)));
            }
          }
          return output;
        }))
        .setCoder(KvCoder.of(
            StringUtf8Coder.of(),
            KvCoder.of(
                AvroCoder.of(AvroGeneratedUser.getClassSchema()),
                TableRowJsonCoder.of())));

    pipeline.run();
  }
}
