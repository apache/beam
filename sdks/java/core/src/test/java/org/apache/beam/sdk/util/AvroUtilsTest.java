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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.util.AvroUtils.AvroMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for AvroUtils.
 */
@RunWith(JUnit4.class)
public class AvroUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final int DEFAULT_RECORD_COUNT = 10000;

  /**
   * Generates an input Avro file containing the given records in the temporary directory and
   * returns the full path of the file.
   */
  @SuppressWarnings("deprecation")  // test of internal functionality
  private <T> String generateTestFile(String filename, List<T> elems, AvroCoder<T> coder,
      String codec) throws IOException {
    File tmpFile = tmpFolder.newFile(filename);
    String path = tmpFile.toString();

    FileOutputStream os = new FileOutputStream(tmpFile);
    DatumWriter<T> datumWriter = coder.createDatumWriter();
    try (DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(CodecFactory.fromString(codec));
      writer.create(coder.getSchema(), os);
      for (T elem : elems) {
        writer.append(elem);
      }
    }
    return path;
  }

  @Test
  public void testReadMetadataWithCodecs() throws Exception {
    // Test reading files generated using all codecs.
    String codecs[] = {DataFileConstants.NULL_CODEC, DataFileConstants.BZIP2_CODEC,
        DataFileConstants.DEFLATE_CODEC, DataFileConstants.SNAPPY_CODEC,
        DataFileConstants.XZ_CODEC};
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);

    for (String codec : codecs) {
      String filename = generateTestFile(
          codec, expected, AvroCoder.of(Bird.class), codec);

      Metadata fileMeta = FileSystems.matchSingleFileSpec(filename);
      AvroMetadata metadata = AvroUtils.readMetadataFromFile(fileMeta.resourceId());
      assertEquals(codec, metadata.getCodec());
    }
  }

  @Test
  public void testReadSchemaString() throws Exception {
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);
    String codec = DataFileConstants.NULL_CODEC;
    String filename = generateTestFile(
        codec, expected, AvroCoder.of(Bird.class), codec);
    Metadata fileMeta = FileSystems.matchSingleFileSpec(filename);
    AvroMetadata metadata = AvroUtils.readMetadataFromFile(fileMeta.resourceId());
    // By default, parse validates the schema, which is what we want.
    Schema schema = new Schema.Parser().parse(metadata.getSchemaString());
    assertEquals(8, schema.getFields().size());
  }

  /**
   * Pojo class used as the record type in tests.
   */
  @DefaultCoder(AvroCoder.class)
  static class Bird {
    long number;
    @Nullable String species;
    @Nullable Double quality;
    @Nullable Long quantity;
    @Nullable Long birthday;  // Exercises TIMESTAMP.
    @Nullable Boolean flighted;
    @Nullable SubBird scion;
    SubBird[] associates;

    static class SubBird {
      @Nullable String species;

      public SubBird() {}
    }

    public Bird() {
      associates = new SubBird[1];
      associates[0] = new SubBird();
    }
  }

  /**
   * Create a list of n random records.
   */
  private static List<Bird> createRandomRecords(long n) {
    String[] species = {"pigeons", "owls", "gulls", "hawks", "robins", "jays"};
    Random random = new Random(0);

    List<Bird> records = new ArrayList<>();
    for (long i = 0; i < n; i++) {
      Bird bird = new Bird();
      bird.quality = random.nextDouble();
      bird.species = species[random.nextInt(species.length)];
      bird.number = i;
      bird.quantity = random.nextLong();
      records.add(bird);
    }
    return records;
  }
}
