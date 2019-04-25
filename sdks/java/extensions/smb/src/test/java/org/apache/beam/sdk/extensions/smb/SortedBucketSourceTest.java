package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
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
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SortedBucketSourceTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder source1Folder = new TemporaryFolder();
  @Rule public final TemporaryFolder source2Folder = new TemporaryFolder();

  static final GenericRecord userA = TestUtils.createUserRecord("a", 50);
  static final GenericRecord userB = TestUtils.createUserRecord("b", 50);
  static final GenericRecord userC = TestUtils.createUserRecord("c", 25);
  static final GenericRecord userD = TestUtils.createUserRecord("d", 25);
  static final GenericRecord userE = TestUtils.createUserRecord("e", 75);

  private SMBFilenamePolicy filenamePolicySource1;
  private SMBFilenamePolicy filenamePolicySource2;

  private final AvroSortedBucketFile<GenericRecord> file = new AvroSortedBucketFile<>(
      GenericRecord.class, TestUtils.schema);

  private static final TupleTag<GenericRecord> SOURCE_1_TAG = new TupleTag<>("source1");
  private static final TupleTag<GenericRecord> SOURCE_2_TAG = new TupleTag<>("source2");

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

    writer1.write(userA);
    writer1.write(userA);
    writer1.write(userC);
    writer1.write(userE);
    writer1.finishWrite();

    Writer<GenericRecord> writer2 = file.createWriter();
    writer2.prepareWrite(FileSystems.create(
        filenamePolicySource2.forDestination()
            .forBucketShard(0, 1, 1, 1),
        file.createWriter().getMimeType()));
    writer2.write(userB);
    writer2.write(userD);
    writer2.finishWrite();

    final BucketMetadata<Integer, GenericRecord> metadata = TestUtils.tryCreateMetadata(1, HashType.MURMUR3_32);

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(
        new File(source1Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);

    objectMapper.writeValue(
        new File(source2Folder.getRoot().getAbsolutePath(), "metadata.json"), metadata);
  }

  static class ToJoinedUserResult extends SMBJoinResult.ToResult<KV<Iterable<GenericRecord>, Iterable<GenericRecord>>> {

    @Override
    Coder<KV<Iterable<GenericRecord>, Iterable<GenericRecord>>> resultCoder() {
      return KvCoder.of(
          NullableCoder.of(IterableCoder.of(AvroCoder.of(TestUtils.schema))),
          NullableCoder.of(IterableCoder.of(AvroCoder.of(TestUtils.schema)))
      );
    }

    @Override
    public KV<Iterable<GenericRecord>, Iterable<GenericRecord>> apply(SMBJoinResult input) {
      return KV.of(
          input.getValuesForTag(SOURCE_1_TAG),
          input.getValuesForTag(SOURCE_2_TAG)
      );
    }
  }
  @Test
  public void testSources() {
    final KeyedBucketSources<Integer> keyedBucketSources = KeyedBucketSources
        .of(pipeline, 1,
            new KeyedBucketSource<Integer, GenericRecord>(
              SOURCE_1_TAG, filenamePolicySource1.forDestination(), file.createReader())
        ).and(
            new KeyedBucketSource<>(SOURCE_2_TAG, filenamePolicySource2.forDestination(), file.createReader())
        );

    final PCollection<KV<Integer, KV<Iterable<GenericRecord>, Iterable<GenericRecord>>>> joinedSources =
        new SortedBucketSource<>(VarIntCoder.of(), new ToJoinedUserResult()).expand(keyedBucketSources);

    PAssert.that(joinedSources).containsInAnyOrder(
        KV.of(50, KV.of(ImmutableList.of(userA, userA), ImmutableList.of(userB))),
        KV.of(25, KV.of(ImmutableList.of(userC), ImmutableList.of(userD))),
        KV.of(75, KV.of(ImmutableList.of(userE), null))
    );

    pipeline.run();
  }
}
