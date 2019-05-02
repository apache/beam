package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonFileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SortedBucketBenchmark {

  public interface SinkOptions extends PipelineOptions {
    String getAvroDestination();
    void setAvroDestination(String value);

    String getJsonDestination();
    void setJsonDestination(String value);
  }

  public static void main(String[] args) throws CannotProvideCoderException {
    SinkOptions sinkOptions = PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);
    Pipeline pipeline = Pipeline.create(sinkOptions);

    int numKeys = 10 * 1000 * 1000;
    int maxRecordsPerKey = 500;
    int numBuckets = 128;

    PCollection<AvroGeneratedUser> avroData = pipeline
        .apply(GenerateSequence.from(0).to(numKeys))
        .apply(FlatMapElements
            .into(TypeDescriptor.of(AvroGeneratedUser.class))
            .via(i -> {
              String userId = String.format("user-%08d", i);
              return IntStream
                  .range(0, new Random().nextInt(maxRecordsPerKey) + 1)
                  .boxed()
                  .map(j -> AvroGeneratedUser.newBuilder()
                      .setName(userId)
                      .setFavoriteNumber(j)
                      .setFavoriteColor(String.format("color-%08d", j))
                      .build())
                  .collect(Collectors.toList());
            }));

    PCollection<TableRow> jsonData = pipeline
        .apply(GenerateSequence.from(0).to(numKeys))
        .apply(FlatMapElements
            .into(TypeDescriptor.of(TableRow.class))
            .via(i -> {
              String userId = String.format("user-%08d", i);
              return IntStream
                  .range(0, new Random().nextInt(maxRecordsPerKey / 10) + 1)
                  .boxed()
                  .map(j -> new TableRow()
                        .set("user", userId)
                        .set("favoritePlace", String.format("place-%08d", j)))
                  .collect(Collectors.toList());
            }))
        .setCoder(TableRowJsonCoder.of());

    ResourceId tempDirectory = FileSystems.matchNewResource(sinkOptions.getTempLocation(), true);

    AvroBucketMetadata<CharSequence, AvroGeneratedUser> avroMetadata =
        new AvroBucketMetadata<>(numBuckets, CharSequence.class, BucketMetadata.HashType.MURMUR3_32, "name");
    SMBFilenamePolicy avroPolicy = new SMBFilenamePolicy(
        FileSystems.matchNewResource(sinkOptions.getAvroDestination(), true), ".avro");
    AvroFileOperations<AvroGeneratedUser> avroOps = new AvroFileOperations<>(AvroGeneratedUser.class, AvroGeneratedUser.getClassSchema());
    SortedBucketSink<CharSequence, AvroGeneratedUser> avroSink =
        new SortedBucketSink<>(avroMetadata, avroPolicy, avroOps::createWriter, tempDirectory);

    avroData.apply(avroSink);

    JsonBucketMetadata<String> jsonMetadata =
        new JsonBucketMetadata<>(numBuckets, String.class, BucketMetadata.HashType.MURMUR3_32, "user");
    SMBFilenamePolicy jsonPolicy = new SMBFilenamePolicy(
        FileSystems.matchNewResource(sinkOptions.getJsonDestination(), true), ".json");
    SortedBucketSink<String, TableRow> jsonSink =
        new SortedBucketSink<>(jsonMetadata, jsonPolicy, () -> new JsonFileOperations().createWriter(), tempDirectory);

    jsonData.apply(jsonSink);

    pipeline.run();
  }
}
