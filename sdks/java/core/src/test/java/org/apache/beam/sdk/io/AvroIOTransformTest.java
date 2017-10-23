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

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

/**
 * A test suite for {@link AvroIO.Write} and {@link AvroIO.Read} transforms.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AvroIOTransformTest.AvroIOReadTransformTest.class,
    AvroIOTransformTest.AvroIOWriteTransformTest.class
})
public class AvroIOTransformTest {

  // TODO: Stop requiring local files

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Schema.Parser parser = new Schema.Parser();
  private static final Schema.Parser parser2 = new Schema.Parser();

  private static final String SCHEMA_STRING_1 =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final String SCHEMA_STRING_2 =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_pet\", \"type\": [\"string\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA_1 = parser.parse(SCHEMA_STRING_1);
  private static final Schema SCHEMA_2 = parser2.parse(SCHEMA_STRING_2);

  private static AvroGeneratedUser[] generateAvroObjects() {
    final AvroGeneratedUser user1 = new AvroGeneratedUser();
    user1.setName("Bob");
    user1.setFavoriteNumber(256);

    final AvroGeneratedUser user2 = new AvroGeneratedUser();
    user2.setName("Alice");
    user2.setFavoriteNumber(128);

    final AvroGeneratedUser user3 = new AvroGeneratedUser();
    user3.setName("Ted");
    user3.setFavoriteColor("white");

    return new AvroGeneratedUser[] { user1, user2, user3 };
  }

  private static GenericRecord[] generateAvroGenericRecords() {
    final GenericRecord user1 = new GenericData.Record(SCHEMA_1);
    user1.put("name", "Bob");
    user1.put("favorite_number", 256);

    final GenericRecord user2 = new GenericData.Record(SCHEMA_1);
    user2.put("name", "Alice");
    user2.put("favorite_number", 128);

    final GenericRecord user3 = new GenericData.Record(SCHEMA_1);
    user3.put("name", "Ted");
    user3.put("favorite_color", "white");

    return new GenericRecord[] { user1, user2, user3 };
  }

  private static GenericRecord[] generateAvroGenericRecordsWithHeterogeneousSchemas() {
    final GenericRecord user1 = new GenericData.Record(SCHEMA_1);
    user1.put("name", "Bob");
    user1.put("favorite_number", 256);

    // one record with different (from the other records) but correct schema
    final GenericRecord user2 = new GenericData.Record(SCHEMA_2);
    user2.put("name", "Alice");
    user2.put("favorite_pet", "cat");

    final GenericRecord user3 = new GenericData.Record(SCHEMA_1);
    user3.put("name", "Ted");
    user3.put("favorite_color", "white");

    return new GenericRecord[] { user1, user2, user3 };
  }

  /**
   * Tests for AvroIO Read transforms, using classes generated from {@code user.avsc}.
   */
  @RunWith(Parameterized.class)
  public static class AvroIOReadTransformTest extends AvroIOTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private void generateAvroFile(final AvroGeneratedUser[] elements,
                                  final File avroFile) throws IOException {
      final DatumWriter<AvroGeneratedUser> userDatumWriter =
          new SpecificDatumWriter<>(AvroGeneratedUser.class);
      try (DataFileWriter<AvroGeneratedUser> dataFileWriter =
          new DataFileWriter<>(userDatumWriter)) {
        dataFileWriter.create(elements[0].getSchema(), avroFile);
        for (final AvroGeneratedUser user : elements) {
          dataFileWriter.append(user);
        }
      }
    }

    private <T> void runTestRead(@Nullable final String applyName,
                                 final AvroIO.Read<T> readBuilder,
                                 final String expectedName,
                                 final T[] expectedOutput) throws Exception {

      final File avroFile = tmpFolder.newFile("file.avro");
      generateAvroFile(generateAvroObjects(), avroFile);
      final AvroIO.Read<T> read = readBuilder.from(avroFile.getPath());
      final PCollection<T> output =
          applyName == null ? pipeline.apply(read) : pipeline.apply(applyName, read);

      PAssert.that(output).containsInAnyOrder(expectedOutput);

      pipeline.run();

      assertEquals(expectedName, output.getName());
    }

    @Parameterized.Parameters(name = "{2}_with_{4}")
    public static Iterable<Object[]> data() throws IOException {

      final String generatedClass = "GeneratedClass";
      final String fromSchema = "SchemaObject";
      final String fromSchemaString = "SchemaString";

      return
          ImmutableList.<Object[]>builder()
              .add(

                  // test read using generated class
                  new Object[] {
                      null,
                      AvroIO.read(AvroGeneratedUser.class),
                      "AvroIO.Read/Read.out",
                      generateAvroObjects(),
                      generatedClass
                  },
                  new Object[] {
                      "MyRead",
                      AvroIO.read(AvroGeneratedUser.class),
                      "MyRead/Read.out",
                      generateAvroObjects(),
                      generatedClass
                  },

                  // test read using schema object
                  new Object[] {
                      null,
                      AvroIO.readGenericRecords(SCHEMA_1),
                      "AvroIO.Read/Read.out",
                      generateAvroGenericRecords(),
                      fromSchema
                  },
                  new Object[] {
                      "MyRead",
                      AvroIO.readGenericRecords(SCHEMA_1),
                      "MyRead/Read.out",
                      generateAvroGenericRecords(),
                      fromSchema
                  },

                  // test read using schema string
                  new Object[] {
                      null,
                      AvroIO.readGenericRecords(SCHEMA_STRING_1),
                      "AvroIO.Read/Read.out",
                      generateAvroGenericRecords(),
                      fromSchemaString
                  },
                  new Object[] {
                      "MyRead",
                      AvroIO.readGenericRecords(SCHEMA_STRING_1),
                      "MyRead/Read.out",
                      generateAvroGenericRecords(),
                      fromSchemaString
                  })
              .build();
    }

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public String transformName;

    @Parameterized.Parameter(1)
    public AvroIO.Read readTransform;

    @Parameterized.Parameter(2)
    public String expectedReadTransformName;

    @Parameterized.Parameter(3)
    public Object[] expectedOutput;

    @Parameterized.Parameter(4)
    public String testAlias;

    @Test
    @Category(NeedsRunner.class)
    public void testRead() throws Exception {
      runTestRead(transformName, readTransform, expectedReadTransformName, expectedOutput);
    }
  }

  /**
   * Tests for AvroIO Write transforms, using classes generated from {@code user.avsc}.
   */
  @RunWith(Parameterized.class)
  public static class AvroIOWriteTransformTest extends AvroIOTransformTest {

    public final transient TestPipeline pipeline = TestPipeline.create();
    public ExpectedException thrown = ExpectedException.none();

    // the ExpectedException must stop the pipeline
    @Rule
    public final transient RuleChain chain = RuleChain.outerRule(thrown).around(pipeline);

    private static final String WRITE_TRANSFORM_NAME = "AvroIO.Write";

    private <T> List<T> readAvroFile(final File avroFile, boolean generic) throws IOException {
      if (!generic) {
        final DatumReader<AvroGeneratedUser> datumReader = new SpecificDatumReader<>(
            AvroGeneratedUser.class);
        final List<AvroGeneratedUser> output = new ArrayList<>();
        try (DataFileReader<AvroGeneratedUser> dataFileReader = new DataFileReader<>(avroFile,
            datumReader)) {
          while (dataFileReader.hasNext()) {
            output.add(dataFileReader.next());
          }
        }
        return (List<T>) output;
      } else {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SCHEMA_1);
        final List<GenericRecord> genericRecords = new ArrayList<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile,
            datumReader)) {
          while (dataFileReader.hasNext()) {
            genericRecords.add(dataFileReader.next());
          }
        }
        return (List<T>) genericRecords;
      }
    }

    @Parameterized.Parameters(name = "{0}_with_{1}")
    public static Iterable<Object[]> data() throws IOException {

      final String generatedClass = "GeneratedClass";
      final String genericRecordsfromSchema = "GenericRecordsFromSchemaObject";
      final String genericRecordsfromSchemaString = "GenericRecordsFromSchemaString";
      final String generatedClassWithoutSchema = "GeneratedClassWithoutSchema";
      final String genericRecordsWithoutSchema = "GenericRecordsWithoutSchema";
      final String genericRecordsWithHeterogeneousSchema = "GenericRecordsWithHeterogeneousSchema";
      final String genericRecordsWithHeterogeneousSchemaWithLazySchemaGuess =
          "GenericRecordsWithHeterogeneousSchemaWithLazySchemaGuess";

      return
          ImmutableList.<Object[]>builder()
              .add(
                  new Object[] {
                      AvroIO.write(AvroGeneratedUser.class),
                      generatedClass,
                      AvroIOTransformTest.generateAvroObjects(),
                      false,
                      false
                  },
                  new Object[] {
                      AvroIO.writeGenericRecords(SCHEMA_1),
                      genericRecordsfromSchema,
                      AvroIOTransformTest.generateAvroGenericRecords(),
                      true,
                      false
                  },
                  new Object[] {
                      AvroIO.writeGenericRecords(SCHEMA_STRING_1),
                      genericRecordsfromSchemaString,
                      AvroIOTransformTest.generateAvroGenericRecords(),
                      true,
                      false
                  },
                  new Object[] {
                      AvroIO.write(),
                      generatedClassWithoutSchema,
                      AvroIOTransformTest.generateAvroObjects(),
                      false,
                      false
                  },
                  new Object[] {
                      AvroIO.writeGenericRecords(),
                      genericRecordsWithoutSchema,
                      AvroIOTransformTest.generateAvroGenericRecords(),
                      true,
                      false
                  },
                  new Object[] {
                      AvroIO.writeGenericRecords(SCHEMA_1),
                      genericRecordsWithHeterogeneousSchema,
                      AvroIOTransformTest.generateAvroGenericRecordsWithHeterogeneousSchemas(),
                      true,
                      true
                  },
                  new Object[] {
                      AvroIO.writeGenericRecords(),
                      genericRecordsWithHeterogeneousSchemaWithLazySchemaGuess,
                      AvroIOTransformTest.generateAvroGenericRecordsWithHeterogeneousSchemas(),
                      true,
                      true
                  })
              .build();
    }

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public AvroIO.Write writeTransform;

    @Parameterized.Parameter(1)
    public String testAlias;

    @Parameterized.Parameter(2)
    public Object[] inputData;

    @Parameterized.Parameter(3)
    public Boolean generic;

    @Parameterized.Parameter(4)
    public boolean expectAvroException;

    private <T> void runTestWrite(final AvroIO.Write<T> writeBuilder, T[] inputData,
        boolean generic, boolean expectAvroException) throws Exception {


      TestPipeline p = pipeline;
      final File avroFile = tmpFolder.newFile("file.avro");
      final AvroIO.Write<T> write = writeBuilder.to(avroFile.getPath());

      if (expectAvroException) {
        thrown.expect(isA(UnresolvedUnionException.class));
      }
      @SuppressWarnings("unchecked") final
      PCollection<T> input =
          p.apply(Create.of(Arrays.asList(inputData))
                               .withCoder((Coder<T>) AvroCoder.of(AvroGeneratedUser.class)));
      input.apply(write.withoutSharding());
      p.run();


      assertEquals(WRITE_TRANSFORM_NAME, write.getName());
      assertThat(readAvroFile(avroFile, generic), containsInAnyOrder((Object[]) inputData));
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWrite() throws Exception {
      runTestWrite(writeTransform, inputData, generic, expectAvroException);
    }

    // TODO: for Write only, test withSuffix, withNumShards,
    // withShardNameTemplate and withoutSharding.
  }
}
