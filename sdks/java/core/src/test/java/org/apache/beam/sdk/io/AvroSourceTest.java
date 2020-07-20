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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroSource.AvroMetadata;
import org.apache.beam.sdk.io.AvroSource.AvroReader;
import org.apache.beam.sdk.io.AvroSource.AvroReader.Seeker;
import org.apache.beam.sdk.io.BlockBasedSource.BlockBasedReader;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for AvroSource. */
@RunWith(JUnit4.class)
public class AvroSourceTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private enum SyncBehavior {
    SYNC_REGULAR, // Sync at regular, user defined intervals
    SYNC_RANDOM, // Sync at random intervals
    SYNC_DEFAULT // Sync at default intervals (i.e., no manual syncing).
  }

  private static final int DEFAULT_RECORD_COUNT = 1000;

  /**
   * Generates an input Avro file containing the given records in the temporary directory and
   * returns the full path of the file.
   */
  private <T> String generateTestFile(
      String filename,
      List<T> elems,
      SyncBehavior syncBehavior,
      int syncInterval,
      AvroCoder<T> coder,
      String codec)
      throws IOException {
    Random random = new Random(0);
    File tmpFile = tmpFolder.newFile(filename);
    String path = tmpFile.toString();

    FileOutputStream os = new FileOutputStream(tmpFile);
    DatumWriter<T> datumWriter =
        coder.getType().equals(GenericRecord.class)
            ? new GenericDatumWriter<>(coder.getSchema())
            : new ReflectDatumWriter<>(coder.getSchema());
    try (DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(CodecFactory.fromString(codec));
      writer.create(coder.getSchema(), os);

      int recordIndex = 0;
      int syncIndex = syncBehavior == SyncBehavior.SYNC_RANDOM ? random.nextInt(syncInterval) : 0;

      for (T elem : elems) {
        writer.append(elem);
        recordIndex++;

        switch (syncBehavior) {
          case SYNC_REGULAR:
            if (recordIndex == syncInterval) {
              recordIndex = 0;
              writer.sync();
            }
            break;
          case SYNC_RANDOM:
            if (recordIndex == syncIndex) {
              recordIndex = 0;
              writer.sync();
              syncIndex = random.nextInt(syncInterval);
            }
            break;
          case SYNC_DEFAULT:
          default:
        }
      }
    }
    return path;
  }

  @Test
  public void testReadWithDifferentCodecs() throws Exception {
    // Test reading files generated using all codecs.
    String[] codecs = {
      DataFileConstants.NULL_CODEC,
      DataFileConstants.BZIP2_CODEC,
      DataFileConstants.DEFLATE_CODEC,
      DataFileConstants.SNAPPY_CODEC,
      DataFileConstants.XZ_CODEC,
    };
    // As Avro's default block size is 64KB, write 64K records to ensure at least one full block.
    // We could make this smaller than 64KB assuming each record is at least B bytes, but then the
    // test could silently stop testing the failure condition from BEAM-422.
    List<Bird> expected = createRandomRecords(1 << 16);

    for (String codec : codecs) {
      String filename =
          generateTestFile(
              codec, expected, SyncBehavior.SYNC_DEFAULT, 0, AvroCoder.of(Bird.class), codec);
      AvroSource<Bird> source = AvroSource.from(filename).withSchema(Bird.class);
      List<Bird> actual = SourceTestUtils.readFromSource(source, null);
      assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    // A reduced dataset is enough here.
    List<FixedRecord> expected = createFixedRecords(DEFAULT_RECORD_COUNT);
    // Create an AvroSource where each block is 1/10th of the total set of records.
    String filename =
        generateTestFile(
            "tmp.avro",
            expected,
            SyncBehavior.SYNC_REGULAR,
            DEFAULT_RECORD_COUNT / 10 /* max records per block */,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);
    File file = new File(filename);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    List<? extends BoundedSource<FixedRecord>> splits = source.split(file.length() / 3, null);
    for (BoundedSource<FixedRecord> subSource : splits) {
      int items = SourceTestUtils.readFromSource(subSource, null).size();
      // Shouldn't split while unstarted.
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.0, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, 1, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, DEFAULT_RECORD_COUNT / 100, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, DEFAULT_RECORD_COUNT / 10, 0.1, null);
      SourceTestUtils.assertSplitAtFractionFails(
          subSource, DEFAULT_RECORD_COUNT / 10 + 1, 0.1, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, DEFAULT_RECORD_COUNT / 3, 0.3, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 0.9, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 1.0, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, items, 0.999, null);
    }
  }

  @Test
  public void testGetProgressFromUnstartedReader() throws Exception {
    List<FixedRecord> records = createFixedRecords(DEFAULT_RECORD_COUNT);
    String filename =
        generateTestFile(
            "tmp.avro",
            records,
            SyncBehavior.SYNC_DEFAULT,
            1000,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);
    File file = new File(filename);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    try (BoundedSource.BoundedReader<FixedRecord> reader = source.createReader(null)) {
      assertEquals(Double.valueOf(0.0), reader.getFractionConsumed());
    }

    List<? extends BoundedSource<FixedRecord>> splits = source.split(file.length() / 3, null);
    for (BoundedSource<FixedRecord> subSource : splits) {
      try (BoundedSource.BoundedReader<FixedRecord> reader = subSource.createReader(null)) {
        assertEquals(Double.valueOf(0.0), reader.getFractionConsumed());
      }
    }
  }

  @Test
  public void testProgress() throws Exception {
    // 5 records, 2 per block.
    List<FixedRecord> records = createFixedRecords(5);
    String filename =
        generateTestFile(
            "tmp.avro",
            records,
            SyncBehavior.SYNC_REGULAR,
            2,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    try (BoundedSource.BoundedReader<FixedRecord> readerOrig = source.createReader(null)) {
      assertThat(readerOrig, Matchers.instanceOf(BlockBasedReader.class));
      BlockBasedReader<FixedRecord> reader = (BlockBasedReader<FixedRecord>) readerOrig;

      // Before starting
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // First 2 records are in the same block.
      assertTrue(reader.start());
      assertTrue(reader.isAtSplitPoint());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());
      // continued
      assertTrue(reader.advance());
      assertFalse(reader.isAtSplitPoint());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Second block -> parallelism consumed becomes 1.
      assertTrue(reader.advance());
      assertTrue(reader.isAtSplitPoint());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());
      // continued
      assertTrue(reader.advance());
      assertFalse(reader.isAtSplitPoint());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Third and final block -> parallelism consumed becomes 2, remaining becomes 1.
      assertTrue(reader.advance());
      assertTrue(reader.isAtSplitPoint());
      assertEquals(2, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // Done
      assertFalse(reader.advance());
      assertEquals(3, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
    }
  }

  @Test
  public void testProgressEmptySource() throws Exception {
    // 0 records, 20 per block.
    List<FixedRecord> records = Collections.emptyList();
    String filename =
        generateTestFile(
            "tmp.avro",
            records,
            SyncBehavior.SYNC_REGULAR,
            2,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    try (BoundedSource.BoundedReader<FixedRecord> readerOrig = source.createReader(null)) {
      assertThat(readerOrig, Matchers.instanceOf(BlockBasedReader.class));
      BlockBasedReader<FixedRecord> reader = (BlockBasedReader<FixedRecord>) readerOrig;

      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // confirm empty
      assertFalse(reader.start());

      // after reading empty source
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
    }
  }

  @Test
  public void testGetCurrentFromUnstartedReader() throws Exception {
    List<FixedRecord> records = createFixedRecords(DEFAULT_RECORD_COUNT);
    String filename =
        generateTestFile(
            "tmp.avro",
            records,
            SyncBehavior.SYNC_DEFAULT,
            1000,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    try (BlockBasedSource.BlockBasedReader<FixedRecord> reader =
        (BlockBasedSource.BlockBasedReader<FixedRecord>) source.createReader(null)) {
      assertEquals(null, reader.getCurrentBlock());

      expectedException.expect(NoSuchElementException.class);
      expectedException.expectMessage("No block has been successfully read from");
      reader.getCurrent();
    }
  }

  @Test
  public void testSplitAtFractionExhaustive() throws Exception {
    // A small-sized input is sufficient, because the test verifies that splitting is non-vacuous.
    List<FixedRecord> expected = createFixedRecords(20);
    String filename =
        generateTestFile(
            "tmp.avro",
            expected,
            SyncBehavior.SYNC_REGULAR,
            5,
            AvroCoder.of(FixedRecord.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<FixedRecord> source = AvroSource.from(filename).withSchema(FixedRecord.class);
    SourceTestUtils.assertSplitAtFractionExhaustive(source, null);
  }

  @Test
  public void testSplitsWithSmallBlocks() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    // Test reading from an object file with many small random-sized blocks.
    // The file itself doesn't have to be big; we can use a decreased record count.
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);
    String filename =
        generateTestFile(
            "tmp.avro",
            expected,
            SyncBehavior.SYNC_RANDOM,
            DEFAULT_RECORD_COUNT / 20 /* max records/block */,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);
    File file = new File(filename);

    // Small minimum bundle size
    AvroSource<Bird> source =
        AvroSource.from(filename).withSchema(Bird.class).withMinBundleSize(100L);

    // Assert that the source produces the expected records
    assertEquals(expected, SourceTestUtils.readFromSource(source, options));

    List<? extends BoundedSource<Bird>> splits;
    int nonEmptySplits;

    // Split with the minimum bundle size
    splits = source.split(100L, options);
    assertTrue(splits.size() > 2);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    nonEmptySplits = 0;
    for (BoundedSource<Bird> subSource : splits) {
      if (SourceTestUtils.readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertTrue(nonEmptySplits > 2);

    // Split with larger bundle size
    splits = source.split(file.length() / 4, options);
    assertTrue(splits.size() > 2);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    nonEmptySplits = 0;
    for (BoundedSource<Bird> subSource : splits) {
      if (SourceTestUtils.readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertTrue(nonEmptySplits > 2);

    // Split with the file length
    splits = source.split(file.length(), options);
    assertTrue(splits.size() == 1);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
  }

  @Test
  public void testMultipleFiles() throws Exception {
    String baseName = "tmp-";
    List<Bird> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<Bird> contents = createRandomRecords(DEFAULT_RECORD_COUNT / 10);
      expected.addAll(contents);
      generateTestFile(
          baseName + i,
          contents,
          SyncBehavior.SYNC_DEFAULT,
          0,
          AvroCoder.of(Bird.class),
          DataFileConstants.NULL_CODEC);
    }

    AvroSource<Bird> source =
        AvroSource.from(new File(tmpFolder.getRoot().toString(), baseName + "*").toString())
            .withSchema(Bird.class);
    List<Bird> actual = SourceTestUtils.readFromSource(source, null);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testCreationWithSchema() throws Exception {
    List<Bird> expected = createRandomRecords(100);
    String filename =
        generateTestFile(
            "tmp.avro",
            expected,
            SyncBehavior.SYNC_DEFAULT,
            0,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);

    // Create a source with a schema object
    Schema schema = ReflectData.get().getSchema(Bird.class);
    AvroSource<GenericRecord> source = AvroSource.from(filename).withSchema(schema);
    List<GenericRecord> records = SourceTestUtils.readFromSource(source, null);
    assertEqualsWithGeneric(expected, records);

    // Create a source with a JSON schema
    String schemaString = ReflectData.get().getSchema(Bird.class).toString();
    source = AvroSource.from(filename).withSchema(schemaString);
    records = SourceTestUtils.readFromSource(source, null);
    assertEqualsWithGeneric(expected, records);
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    List<Bird> birds = createRandomRecords(100);
    String filename =
        generateTestFile(
            "tmp.avro",
            birds,
            SyncBehavior.SYNC_DEFAULT,
            0,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<FancyBird> source = AvroSource.from(filename).withSchema(FancyBird.class);
    List<FancyBird> actual = SourceTestUtils.readFromSource(source, null);

    List<FancyBird> expected = new ArrayList<>();
    for (Bird bird : birds) {
      expected.add(
          new FancyBird(
              bird.number, bird.species, bird.quality, bird.quantity, null, "MAXIMUM OVERDRIVE"));
    }

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testSchemaStringIsInterned() throws Exception {
    List<Bird> birds = createRandomRecords(100);
    String filename =
        generateTestFile(
            "tmp.avro",
            birds,
            SyncBehavior.SYNC_DEFAULT,
            0,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);
    Metadata fileMetadata = FileSystems.matchSingleFileSpec(filename);
    String schema = AvroSource.readMetadataFromFile(fileMetadata.resourceId()).getSchemaString();
    // Add "" to the schema to make sure it is not interned.
    AvroSource<GenericRecord> sourceA = AvroSource.from(filename).withSchema("" + schema);
    AvroSource<GenericRecord> sourceB = AvroSource.from(filename).withSchema("" + schema);
    assertSame(sourceA.getReaderSchemaString(), sourceB.getReaderSchemaString());

    // Ensure that deserialization still goes through interning
    AvroSource<GenericRecord> sourceC = SerializableUtils.clone(sourceB);
    assertSame(sourceA.getReaderSchemaString(), sourceC.getReaderSchemaString());
  }

  @Test
  public void testParseFn() throws Exception {
    List<Bird> expected = createRandomRecords(100);
    String filename =
        generateTestFile(
            "tmp.avro",
            expected,
            SyncBehavior.SYNC_DEFAULT,
            0,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);

    AvroSource<Bird> source =
        AvroSource.from(filename)
            .withParseFn(
                input ->
                    new Bird(
                        (long) input.get("number"),
                        input.get("species").toString(),
                        input.get("quality").toString(),
                        (long) input.get("quantity")),
                AvroCoder.of(Bird.class));
    List<Bird> actual = SourceTestUtils.readFromSource(source, null);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testDatumReaderFactoryWithGenericRecord() throws Exception {
    List<Bird> inputBirds = createRandomRecords(100);

    String filename =
        generateTestFile(
            "tmp.avro",
            inputBirds,
            SyncBehavior.SYNC_DEFAULT,
            0,
            AvroCoder.of(Bird.class),
            DataFileConstants.NULL_CODEC);

    AvroSource.DatumReaderFactory<GenericRecord> factory =
        (writer, reader) ->
            new GenericDatumReader<GenericRecord>(writer, reader) {
              @Override
              protected Object readString(Object old, Decoder in) throws IOException {
                return super.readString(old, in) + "_custom";
              }
            };

    AvroSource<Bird> source =
        AvroSource.from(filename)
            .withParseFn(
                input ->
                    new Bird(
                        (long) input.get("number"),
                        input.get("species").toString(),
                        input.get("quality").toString(),
                        (long) input.get("quantity")),
                AvroCoder.of(Bird.class))
            .withDatumReaderFactory(factory);
    List<Bird> actual = SourceTestUtils.readFromSource(source, null);
    List<Bird> expected =
        inputBirds.stream()
            .map(b -> new Bird(b.number, b.species + "_custom", b.quality + "_custom", b.quantity))
            .collect(Collectors.toList());

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  private void assertEqualsWithGeneric(List<Bird> expected, List<GenericRecord> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Bird fixed = expected.get(i);
      GenericRecord generic = actual.get(i);
      assertEquals(fixed.number, generic.get("number"));
      assertEquals(fixed.quality, generic.get("quality").toString()); // From Avro util.Utf8
      assertEquals(fixed.quantity, generic.get("quantity"));
      assertEquals(fixed.species, generic.get("species").toString());
    }
  }

  /**
   * Creates a haystack byte array of the give size with a needle that starts at the given position.
   */
  private byte[] createHaystack(byte[] needle, int position, int size) {
    byte[] haystack = new byte[size];
    for (int i = position, j = 0; i < size && j < needle.length; i++, j++) {
      haystack[i] = needle[j];
    }
    return haystack;
  }

  /**
   * Asserts that advancePastNextSyncMarker advances an input stream past a sync marker and
   * correctly returns the number of bytes consumed from the stream. Creates a haystack of size
   * bytes and places a 16-byte sync marker at the position specified.
   */
  private void testAdvancePastNextSyncMarkerAt(int position, int size) throws IOException {
    byte sentinel = (byte) 0xFF;
    byte[] marker = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
    byte[] haystack = createHaystack(marker, position, size);
    PushbackInputStream stream =
        new PushbackInputStream(new ByteArrayInputStream(haystack), marker.length);
    if (position + marker.length < size) {
      haystack[position + marker.length] = sentinel;
      assertEquals(position + marker.length, AvroReader.advancePastNextSyncMarker(stream, marker));
      assertEquals(sentinel, (byte) stream.read());
    } else {
      assertEquals(size, AvroReader.advancePastNextSyncMarker(stream, marker));
      assertEquals(-1, stream.read());
    }
  }

  @Test
  public void testAdvancePastNextSyncMarker() throws IOException {
    // Test placing the sync marker at different locations at the start and in the middle of the
    // buffer.
    for (int i = 0; i <= 16; i++) {
      testAdvancePastNextSyncMarkerAt(i, 1000);
      testAdvancePastNextSyncMarkerAt(160 + i, 1000);
    }
    // Test placing the sync marker at the end of the buffer.
    testAdvancePastNextSyncMarkerAt(983, 1000);
    // Test placing the sync marker so that it begins at the end of the buffer.
    testAdvancePastNextSyncMarkerAt(984, 1000);
    testAdvancePastNextSyncMarkerAt(985, 1000);
    testAdvancePastNextSyncMarkerAt(999, 1000);
    // Test with no sync marker.
    testAdvancePastNextSyncMarkerAt(1000, 1000);
  }

  // Tests for Seeker.
  @Test
  public void testSeekerFind() {
    byte[] marker = {0, 1, 2, 3};
    byte[] buffer;
    Seeker s;
    s = new Seeker(marker);

    buffer = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};
    assertEquals(3, s.find(buffer, buffer.length));

    buffer = new byte[] {0, 0, 0, 0, 0, 1, 2, 3};
    assertEquals(7, s.find(buffer, buffer.length));

    buffer = new byte[] {0, 1, 2, 0, 0, 1, 2, 3};
    assertEquals(7, s.find(buffer, buffer.length));

    buffer = new byte[] {0, 1, 2, 3};
    assertEquals(3, s.find(buffer, buffer.length));
  }

  @Test
  public void testSeekerFindResume() {
    byte[] marker = {0, 1, 2, 3};
    byte[] buffer;
    Seeker s;
    s = new Seeker(marker);

    buffer = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {1, 2, 3, 0, 0, 0, 0, 0};
    assertEquals(2, s.find(buffer, buffer.length));

    buffer = new byte[] {0, 0, 0, 0, 0, 0, 1, 2};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {3, 0, 1, 2, 3, 0, 1, 2};
    assertEquals(0, s.find(buffer, buffer.length));

    buffer = new byte[] {0};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {1};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {2};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {3};
    assertEquals(0, s.find(buffer, buffer.length));
  }

  @Test
  public void testSeekerUsesBufferLength() {
    byte[] marker = {0, 0, 1};
    byte[] buffer;
    Seeker s;
    s = new Seeker(marker);

    buffer = new byte[] {0, 0, 0, 1};
    assertEquals(-1, s.find(buffer, 3));

    s = new Seeker(marker);
    buffer = new byte[] {0, 0};
    assertEquals(-1, s.find(buffer, 1));
    buffer = new byte[] {1, 0};
    assertEquals(-1, s.find(buffer, 1));

    s = new Seeker(marker);
    buffer = new byte[] {0, 2};
    assertEquals(-1, s.find(buffer, 1));
    buffer = new byte[] {0, 2};
    assertEquals(-1, s.find(buffer, 1));
    buffer = new byte[] {1, 2};
    assertEquals(0, s.find(buffer, 1));
  }

  @Test
  public void testSeekerFindPartial() {
    byte[] marker = {0, 0, 1};
    byte[] buffer;
    Seeker s;
    s = new Seeker(marker);

    buffer = new byte[] {0, 0, 0, 1};
    assertEquals(3, s.find(buffer, buffer.length));

    marker = new byte[] {1, 1, 1, 2};
    s = new Seeker(marker);

    buffer = new byte[] {1, 1, 1, 1, 1};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {1, 1, 2};
    assertEquals(2, s.find(buffer, buffer.length));

    buffer = new byte[] {1, 1, 1, 1, 1};
    assertEquals(-1, s.find(buffer, buffer.length));
    buffer = new byte[] {2, 1, 1, 1, 2};
    assertEquals(0, s.find(buffer, buffer.length));
  }

  @Test
  public void testSeekerFindAllLocations() {
    byte[] marker = {1, 1, 2};
    byte[] allOnes = new byte[] {1, 1, 1, 1};
    byte[] findIn = new byte[] {1, 1, 1, 1};
    Seeker s = new Seeker(marker);

    for (int i = 0; i < findIn.length; i++) {
      assertEquals(-1, s.find(allOnes, allOnes.length));
      findIn[i] = 2;
      assertEquals(i, s.find(findIn, findIn.length));
      findIn[i] = 1;
    }
  }

  @Test
  public void testDisplayData() {
    AvroSource<Bird> source =
        AvroSource.from("foobar.txt").withSchema(Bird.class).withMinBundleSize(1234);

    DisplayData displayData = DisplayData.from(source);
    assertThat(displayData, hasDisplayItem("filePattern", "foobar.txt"));
    assertThat(displayData, hasDisplayItem("minBundleSize", 1234));
  }

  @Test
  public void testReadMetadataWithCodecs() throws Exception {
    // Test reading files generated using all codecs.
    String[] codecs = {
      DataFileConstants.NULL_CODEC,
      DataFileConstants.BZIP2_CODEC,
      DataFileConstants.DEFLATE_CODEC,
      DataFileConstants.SNAPPY_CODEC,
      DataFileConstants.XZ_CODEC
    };
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);

    for (String codec : codecs) {
      String filename =
          generateTestFile(
              codec, expected, SyncBehavior.SYNC_DEFAULT, 0, AvroCoder.of(Bird.class), codec);

      Metadata fileMeta = FileSystems.matchSingleFileSpec(filename);
      AvroMetadata metadata = AvroSource.readMetadataFromFile(fileMeta.resourceId());
      assertEquals(codec, metadata.getCodec());
    }
  }

  @Test
  public void testReadSchemaString() throws Exception {
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);
    String codec = DataFileConstants.NULL_CODEC;
    String filename =
        generateTestFile(
            codec, expected, SyncBehavior.SYNC_DEFAULT, 0, AvroCoder.of(Bird.class), codec);
    Metadata fileMeta = FileSystems.matchSingleFileSpec(filename);
    AvroMetadata metadata = AvroSource.readMetadataFromFile(fileMeta.resourceId());
    // By default, parse validates the schema, which is what we want.
    Schema schema = new Schema.Parser().parse(metadata.getSchemaString());
    assertEquals(4, schema.getFields().size());
  }

  @Test
  public void testCreateFromMetadata() throws Exception {
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);
    String codec = DataFileConstants.NULL_CODEC;
    String filename =
        generateTestFile(
            codec, expected, SyncBehavior.SYNC_DEFAULT, 0, AvroCoder.of(Bird.class), codec);
    Metadata fileMeta = FileSystems.matchSingleFileSpec(filename);

    AvroSource<GenericRecord> source = AvroSource.from(fileMeta);
    AvroSource<Bird> sourceWithSchema = source.withSchema(Bird.class);
    AvroSource<Bird> sourceWithSchemaWithMinBundleSize = sourceWithSchema.withMinBundleSize(1234);

    assertEquals(FileBasedSource.Mode.SINGLE_FILE_OR_SUBRANGE, source.getMode());
    assertEquals(FileBasedSource.Mode.SINGLE_FILE_OR_SUBRANGE, sourceWithSchema.getMode());
    assertEquals(
        FileBasedSource.Mode.SINGLE_FILE_OR_SUBRANGE, sourceWithSchemaWithMinBundleSize.getMode());
  }

  /**
   * Class that will encode to a fixed size: 16 bytes.
   *
   * <p>Each object has a 15-byte array. Avro encodes an object of this type as a byte array, so
   * each encoded object will consist of 1 byte that encodes the length of the array, followed by 15
   * bytes.
   */
  @DefaultCoder(AvroCoder.class)
  public static class FixedRecord {
    private byte[] value = new byte[15];

    public FixedRecord() {
      this(0);
    }

    public FixedRecord(int i) {
      value[0] = (byte) i;
      value[1] = (byte) (i >> 8);
      value[2] = (byte) (i >> 16);
      value[3] = (byte) (i >> 24);
    }

    public int asInt() {
      return value[0] | (value[1] << 8) | (value[2] << 16) | (value[3] << 24);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof FixedRecord) {
        FixedRecord other = (FixedRecord) o;
        return this.asInt() == other.asInt();
      }
      return false;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public String toString() {
      return Integer.toString(this.asInt());
    }
  }

  /** Create a list of count 16-byte records. */
  private static List<FixedRecord> createFixedRecords(int count) {
    List<FixedRecord> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      records.add(new FixedRecord(i));
    }
    return records;
  }

  /** Class used as the record type in tests. */
  @DefaultCoder(AvroCoder.class)
  static class Bird {
    long number;
    String species;
    String quality;
    long quantity;

    public Bird() {}

    public Bird(long number, String species, String quality, long quantity) {
      this.number = number;
      this.species = species;
      this.quality = quality;
      this.quantity = quantity;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Bird.class)
          .addValue(number)
          .addValue(species)
          .addValue(quantity)
          .addValue(quality)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Bird) {
        Bird other = (Bird) obj;
        return Objects.equals(species, other.species)
            && Objects.equals(quality, other.quality)
            && quantity == other.quantity
            && number == other.number;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(number, species, quality, quantity);
    }
  }

  /**
   * Class used as the record type in tests.
   *
   * <p>Contains nullable fields and fields with default values. Can be read using a file written
   * with the Bird schema.
   */
  @DefaultCoder(AvroCoder.class)
  public static class FancyBird {
    long number;
    String species;
    String quality;
    long quantity;

    @org.apache.avro.reflect.Nullable String habitat;

    @AvroDefault("\"MAXIMUM OVERDRIVE\"")
    String fancinessLevel;

    public FancyBird() {}

    public FancyBird(
        long number,
        String species,
        String quality,
        long quantity,
        String habitat,
        String fancinessLevel) {
      this.number = number;
      this.species = species;
      this.quality = quality;
      this.quantity = quantity;
      this.habitat = habitat;
      this.fancinessLevel = fancinessLevel;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(FancyBird.class)
          .addValue(number)
          .addValue(species)
          .addValue(quality)
          .addValue(quantity)
          .addValue(habitat)
          .addValue(fancinessLevel)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FancyBird) {
        FancyBird other = (FancyBird) obj;
        return Objects.equals(species, other.species)
            && Objects.equals(quality, other.quality)
            && quantity == other.quantity
            && number == other.number
            && Objects.equals(fancinessLevel, other.fancinessLevel)
            && Objects.equals(habitat, other.habitat);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(number, species, quality, quantity, habitat, fancinessLevel);
    }
  }

  /** Create a list of n random records. */
  private static List<Bird> createRandomRecords(long n) {
    String[] qualities = {
      "miserable", "forelorn", "fidgity", "squirrelly", "fanciful", "chipper", "lazy"
    };
    String[] species = {"pigeons", "owls", "gulls", "hawks", "robins", "jays"};
    Random random = new Random(0);

    List<Bird> records = new ArrayList<>();
    for (long i = 0; i < n; i++) {
      Bird bird = new Bird();
      bird.quality = qualities[random.nextInt(qualities.length)];
      bird.species = species[random.nextInt(species.length)];
      bird.number = i;
      bird.quantity = random.nextLong();
      records.add(bird);
    }
    return records;
  }
}
