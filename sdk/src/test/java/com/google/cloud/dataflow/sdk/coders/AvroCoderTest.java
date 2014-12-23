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

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Tests for AvroCoder.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class AvroCoderTest {

  @DefaultCoder(AvroCoder.class)
  private static class Pojo {
    public String text;
    public int count;

    // Empty constructor required for Avro decoding.
    public Pojo() {
    }

    public Pojo(String text, int count) {
      this.text = text;
      this.count = count;
    }

    // auto-generated
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Pojo pojo = (Pojo) o;

      if (count != pojo.count) {
        return false;
      }
      if (text != null
          ? !text.equals(pojo.text)
          : pojo.text != null) {
        return false;
      }

      return true;
    }

    @Override
    public String toString() {
      return "Pojo{"
          + "text='" + text + '\''
          + ", count=" + count
          + '}';
    }
  }

  static class GetTextFn extends DoFn<Pojo, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().text);
    }
  }

  @Test
  public void testAvroCoderEncoding() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    CloudObject encoding = coder.asCloudObject();

    Assert.assertThat(encoding.keySet(),
        Matchers.containsInAnyOrder("@type", "type", "schema"));
  }

  @Test
  public void testPojoEncoding() throws Exception {
    Pojo value = new Pojo("Hello", 42);
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

    CoderProperties.coderDecodeEncodeEqual(coder, value);
  }

  @Test
  public void testGenericRecordEncoding() throws Exception {
    String schemaString =
        "{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"User\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"name\", \"type\": \"string\"},\n"
      + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
      + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
      + " ]\n"
      + "}";
    Schema schema = (new Schema.Parser()).parse(schemaString);

    GenericRecord before = new GenericData.Record(schema);
    before.put("name", "Bob");
    before.put("favorite_number", 256);
    // Leave favorite_color null

    AvroCoder<GenericRecord> coder = AvroCoder.of(GenericRecord.class, schema);

    CoderProperties.coderDecodeEncodeEqual(coder, before);
    Assert.assertEquals(schema, coder.getSchema());
  }

  @Test
  public void testEncodingNotBuffered() throws Exception {
    // This test ensures that the coder doesn't read ahead and buffer data.
    // Reading ahead causes a problem if the stream consists of records of different
    // types.
    Pojo before = new Pojo("Hello", 42);

    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    SerializableCoder<Integer> intCoder = SerializableCoder.of(Integer.class);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    Context context = Context.NESTED;
    coder.encode(before, outStream, context);
    intCoder.encode(10, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

    Pojo after = coder.decode(inStream, context);
    Assert.assertEquals(before, after);

    Integer intAfter = intCoder.decode(inStream, context);
    Assert.assertEquals(new Integer(10), intAfter);
  }

  @Test
  public void testDefaultCoder() throws Exception {
    Pipeline p = TestPipeline.create();

    // Use MyRecord as input and output types without explicitly specifying
    // a coder (this uses the default coders, which may not be AvroCoder).
    PCollection<String> output =
        p.apply(Create.of(new Pojo("hello", 1), new Pojo("world", 2)))
            .apply(ParDo.of(new GetTextFn()));

    DataflowAssert.that(output)
        .containsInAnyOrder("hello", "world");
    p.run();
  }
}
