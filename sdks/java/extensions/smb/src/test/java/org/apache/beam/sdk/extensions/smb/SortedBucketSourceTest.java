package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources.KeyedBucketSource;
import org.apache.beam.sdk.extensions.smb.avro.AvroSortedBucketFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SortedBucketSourceTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder source1Folder = new TemporaryFolder();
  @Rule public final TemporaryFolder source2Folder = new TemporaryFolder();

  static final GenericRecord user1 = TestUtils.createUserRecord("a", 50);
  static final GenericRecord user2 = TestUtils.createUserRecord("b", 50);
  static final GenericRecord user3 = TestUtils.createUserRecord("c", 25);
  static final GenericRecord user4 = TestUtils.createUserRecord("d", 25);
  static final GenericRecord user5 = TestUtils.createUserRecord("e", 75);

  private SMBFilenamePolicy filenamePolicySource1;
  private SMBFilenamePolicy filenamePolicySource2;

  private final AvroSortedBucketFile<GenericRecord> file = new AvroSortedBucketFile<>(
      GenericRecord.class, TestUtils.schema);

  // Write two sources with GenericRecords + metadata
  @Before
  public void setUp() throws Exception {
    filenamePolicySource1 = new SMBFilenamePolicy(
        LocalResources.fromFile(source1Folder.getRoot(), true),
        "avro",
        null
    );

    filenamePolicySource2 = new SMBFilenamePolicy(
        LocalResources.fromFile(source2Folder.getRoot(), true),
        "avro",
        null
    );

    Writer<GenericRecord> writer1 = file.createWriter();

    writer1.prepareWrite(FileSystems.create(
        filenamePolicySource1.forDestination()
            .forBucketShard(0, 1, 1, 1),
        file.createWriter().getMimeType()));

    writer1.write(user1);
    writer1.write(user3);
    writer1.write(user5);
    writer1.finishWrite();

    Writer<GenericRecord> writer2 = file.createWriter();
    writer2.prepareWrite(FileSystems.create(
        filenamePolicySource2.forDestination()
            .forBucketShard(0, 1, 1, 1),
        file.createWriter().getMimeType()));
    writer2.write(user2);
    writer2.write(user4);
    writer2.finishWrite();

    final BucketMetadata<Integer, GenericRecord> metadata = TestUtils.tryCreateMetadata(1, HashType.MURMUR3_32);

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(
        new File(source1Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    objectMapper.writeValue(
        new File(source2Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);
  }

  @Test
  public void testSources() {
    final KeyedBucketSources<Integer> keyedBucketSources = KeyedBucketSources
        .of(pipeline, 1,
            new KeyedBucketSource<Integer, GenericRecord>(
              new TupleTag<GenericRecord>("source1"), filenamePolicySource1.forDestination(),
                file.createReader())
        ).and(
            new KeyedBucketSource<Integer, GenericRecord>(
                new TupleTag<GenericRecord>("source2"), filenamePolicySource2.forDestination(),
                file.createReader())
        );

    PCollection<KV<Integer, SMBJoinResult>> joinedSources = new SortedBucketSource<Integer>(
        VarIntCoder.of()
    ).expand(keyedBucketSources);

    PAssert.that(joinedSources).satisfies(result -> {
      result.iterator().forEachRemaining(System.out::println);
      return null;
    });

    pipeline.run();
  }
}
