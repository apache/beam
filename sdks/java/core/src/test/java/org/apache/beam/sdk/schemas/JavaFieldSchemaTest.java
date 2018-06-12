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

package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Test;

public class JavaFieldSchemaTest {
  @DefaultSchema(JavaFieldSchema.class)
  public static class SimplePojo {
    public String str;
    public byte aByte;
    public short aShort;
    public int anInt;
    public long aLong;
    public boolean aBoolean;
    public DateTime dateTime;
    public byte[] bytes;

    public SimplePojo() { }

    public SimplePojo(String str, byte aByte, short aShort, int anInt, long aLong, boolean aBoolean,
                      DateTime dateTime, byte[] bytes) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.bytes = bytes;
    }
  }

  static final Schema SIMPLE_SCHEMA = Schema.builder()
      .addStringField("str")
      .addByteField("aByte")
      .addInt16Field("aShort")
      .addInt32Field("anInt")
      .addInt64Field("aLong")
      .addBooleanField("aBoolean")
      .addDateTimeField("dateTime")
      .addByteArrayField("bytes")
      .build();

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimplePojo.class);
    assertEquals(SIMPLE_SCHEMA, schema);
  }

  @Test
  public void testToRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimplePojo pojo = new SimplePojo("string", (byte) 1, (short) 2, 3, 4L,true,
        DateTime.parse("1979-03-14"), "bytearray".getBytes(Charset.defaultCharset()));
    Row row = registry.getToRowFunction(SimplePojo.class).apply(pojo);

    assertEquals(8, row.getFieldCount());
    assertEquals("string", row.getString(0));
    assertEquals((byte) 1, row.getByte(1).byteValue());
    assertEquals((short) 2, row.getInt16(2).shortValue());
    assertEquals((int) 3, row.getInt32(3).intValue());
    assertEquals((long) 4, row.getInt64(4).longValue());
    assertEquals(true, row.getBoolean(5));
    assertEquals(DateTime.parse("1979-03-14"), row.getDateTime(6));
  }

  @Test
  public void testFromRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = Row.withSchema(SIMPLE_SCHEMA)
        .addValues("string", (byte) 1, (short) 2, 3, 4L, true,
            DateTime.parse("1979-03-14"), "bytearray".getBytes(Charset.defaultCharset()))
        .build();

    SimplePojo pojo = registry.getFromRowFunction(SimplePojo.class).apply(row);
    assertEquals("string", pojo.str);
    assertEquals((byte) 1, pojo.aByte);
    assertEquals((short) 2, pojo.aShort);
    assertEquals((int) 3, pojo.anInt);
    assertEquals((long) 4, pojo.aLong);
    assertEquals(true, pojo.aBoolean);
    assertEquals(DateTime.parse("1979-03-14"), pojo.dateTime);
  }
}
