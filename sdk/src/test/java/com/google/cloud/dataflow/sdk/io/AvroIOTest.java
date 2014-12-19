/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for AvroIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class AvroIOTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private File avroFile;

  @Before
  public void prepareAvroFileBeforeAnyTest() throws IOException {
    avroFile = tmpFolder.newFile("file.avro");
  }

  private final String schemaString =
      "{\"namespace\": \"example.avro\",\n"
    + " \"type\": \"record\",\n"
    + " \"name\": \"User\",\n"
    + " \"fields\": [\n"
    + "     {\"name\": \"name\", \"type\": \"string\"},\n"
    + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
    + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
    + " ]\n"
    + "}";
  private final Schema.Parser parser = new Schema.Parser();
  private final Schema schema = parser.parse(schemaString);

  private User[] generateAvroObjects() {
    User user1 = new User();
    user1.setName("Bob");
    user1.setFavoriteNumber(256);

    User user2 = new User();
    user2.setName("Alice");
    user2.setFavoriteNumber(128);

    User user3 = new User();
    user3.setName("Ted");
    user3.setFavoriteColor("white");

    return new User[] { user1, user2, user3 };
  }

  private GenericRecord[] generateAvroGenericRecords() {
    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Bob");
    user1.put("favorite_number", 256);

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Alice");
    user2.put("favorite_number", 128);

    GenericRecord user3 = new GenericData.Record(schema);
    user3.put("name", "Ted");
    user3.put("favorite_color", "white");

    return new GenericRecord[] { user1, user2, user3 };
  }

  private void generateAvroFile(User[] elements) throws IOException {
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
    dataFileWriter.create(elements[0].getSchema(), avroFile);
    for (User user : elements) {
      dataFileWriter.append(user);
    }
    dataFileWriter.close();
  }

  private List<User> readAvroFile() throws IOException {
    DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
    DataFileReader<User> dataFileReader = new DataFileReader<>(avroFile, userDatumReader);
    List<User> users = new ArrayList<>();
    while (dataFileReader.hasNext()) {
      users.add(dataFileReader.next());
    }
    return users;
  }

  <T> void runTestRead(AvroIO.Read.Bound<T> read, String expectedName, T[] expectedOutput)
      throws Exception {
    generateAvroFile(generateAvroObjects());

    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<T> output = p.apply(read);
    EvaluationResults results = p.run();
    assertEquals(expectedName, output.getName());
    assertThat(results.getPCollection(output),
               containsInAnyOrder(expectedOutput));
  }

  @Test
  public void testReadFromGeneratedClass() throws Exception {
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(User.class),
                "AvroIO.Read.out", generateAvroObjects());
    runTestRead(AvroIO.Read.withSchema(User.class)
                           .from(avroFile.getPath()),
                "AvroIO.Read.out", generateAvroObjects());
    runTestRead(AvroIO.Read.named("MyRead")
                           .from(avroFile.getPath())
                           .withSchema(User.class),
                "MyRead.out", generateAvroObjects());
    runTestRead(AvroIO.Read.named("MyRead")
                           .withSchema(User.class)
                           .from(avroFile.getPath()),
                "MyRead.out", generateAvroObjects());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(User.class)
                           .named("HerRead"),
                "HerRead.out", generateAvroObjects());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .named("HerRead")
                           .withSchema(User.class),
                "HerRead.out", generateAvroObjects());
    runTestRead(AvroIO.Read.withSchema(User.class)
                           .named("HerRead")
                           .from(avroFile.getPath()),
                "HerRead.out", generateAvroObjects());
    runTestRead(AvroIO.Read.withSchema(User.class)
                           .from(avroFile.getPath())
                           .named("HerRead"),
                "HerRead.out", generateAvroObjects());
  }

  @Test
  public void testReadFromSchema() throws Exception {
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(schema),
                "AvroIO.Read.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schema)
                           .from(avroFile.getPath()),
                "AvroIO.Read.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.named("MyRead")
                           .from(avroFile.getPath())
                           .withSchema(schema),
                "MyRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.named("MyRead")
                           .withSchema(schema)
                           .from(avroFile.getPath()),
                "MyRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(schema)
                           .named("HerRead"),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .named("HerRead")
                           .withSchema(schema),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schema)
                           .named("HerRead")
                           .from(avroFile.getPath()),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schema)
                           .from(avroFile.getPath())
                           .named("HerRead"),
                "HerRead.out", generateAvroGenericRecords());
  }

  @Test
  public void testReadFromSchemaString() throws Exception {
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(schemaString),
                "AvroIO.Read.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schemaString)
                           .from(avroFile.getPath()),
                "AvroIO.Read.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.named("MyRead")
                           .from(avroFile.getPath())
                           .withSchema(schemaString),
                "MyRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.named("MyRead")
                           .withSchema(schemaString)
                           .from(avroFile.getPath()),
                "MyRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .withSchema(schemaString)
                           .named("HerRead"),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.from(avroFile.getPath())
                           .named("HerRead")
                           .withSchema(schemaString),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schemaString)
                           .named("HerRead")
                           .from(avroFile.getPath()),
                "HerRead.out", generateAvroGenericRecords());
    runTestRead(AvroIO.Read.withSchema(schemaString)
                           .from(avroFile.getPath())
                           .named("HerRead"),
                "HerRead.out", generateAvroGenericRecords());
  }

  <T> void runTestWrite(AvroIO.Write.Bound<T> write, String expectedName)
      throws Exception {
    User[] users = generateAvroObjects();

    DirectPipeline p = DirectPipeline.createForTest();
    @SuppressWarnings("unchecked")
    PCollection<T> input = p.apply(Create.of(Arrays.asList((T[]) users)))
                            .setCoder((Coder<T>) AvroCoder.of(User.class));
    PDone output = input.apply(write.withoutSharding());
    EvaluationResults results = p.run();
    assertEquals(expectedName, write.getName());

    assertThat(readAvroFile(), containsInAnyOrder(users));
  }

  @Test
  public void testWriteFromGeneratedClass() throws Exception {
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(User.class),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.withSchema(User.class)
                             .to(avroFile.getPath()),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .to(avroFile.getPath())
                             .withSchema(User.class),
                 "MyWrite");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .withSchema(User.class)
                             .to(avroFile.getPath()),
                 "MyWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(User.class)
                             .named("HerWrite"),
                 "HerWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .named("HerWrite")
                             .withSchema(User.class),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(User.class)
                             .named("HerWrite")
                             .to(avroFile.getPath()),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(User.class)
                             .to(avroFile.getPath())
                             .named("HerWrite"),
                 "HerWrite");
  }

  @Test
  public void testWriteFromSchema() throws Exception {
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(schema),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.withSchema(schema)
                             .to(avroFile.getPath()),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .to(avroFile.getPath())
                             .withSchema(schema),
                 "MyWrite");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .withSchema(schema)
                             .to(avroFile.getPath()),
                 "MyWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(schema)
                             .named("HerWrite"),
                 "HerWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .named("HerWrite")
                             .withSchema(schema),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(schema)
                             .named("HerWrite")
                             .to(avroFile.getPath()),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(schema)
                             .to(avroFile.getPath())
                             .named("HerWrite"),
                 "HerWrite");
  }

  @Test
  public void testWriteFromSchemaString() throws Exception {
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(schemaString),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.withSchema(schemaString)
                             .to(avroFile.getPath()),
                 "AvroIO.Write");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .to(avroFile.getPath())
                             .withSchema(schemaString),
                 "MyWrite");
    runTestWrite(AvroIO.Write.named("MyWrite")
                             .withSchema(schemaString)
                             .to(avroFile.getPath()),
                 "MyWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .withSchema(schemaString)
                             .named("HerWrite"),
                 "HerWrite");
    runTestWrite(AvroIO.Write.to(avroFile.getPath())
                             .named("HerWrite")
                             .withSchema(schemaString),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(schemaString)
                             .named("HerWrite")
                             .to(avroFile.getPath()),
                 "HerWrite");
    runTestWrite(AvroIO.Write.withSchema(schemaString)
                             .to(avroFile.getPath())
                             .named("HerWrite"),
                 "HerWrite");
  }

  // TODO: for Write only, test withSuffix, withNumShards,
  // withShardNameTemplate and withoutSharding.
}
