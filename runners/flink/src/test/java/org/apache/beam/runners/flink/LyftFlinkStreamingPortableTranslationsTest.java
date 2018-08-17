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
package org.apache.beam.runners.flink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.TimeZone;
import java.util.zip.Deflater;
import org.apache.beam.runners.flink.LyftFlinkStreamingPortableTranslations.LyftBase64ZlibJsonSchema;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Assert;
import org.junit.Test;

public class LyftFlinkStreamingPortableTranslationsTest {

  @Test
  public void testBeamKinesisSchema() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZ"
                    + "GFroGhroGpkrGBhYGRlYGRjpWRoYKNXGAgARiA/1");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaLongTimestamp() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uL" + "SpKTYlPLAGJmJqYGBhbGlsYmhlZ1MYCAGYeDek=");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1544039381628L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaNoTimestamp() throws IOException {
    byte[] message = encode("[{\"event_id\": 1}]");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(Long.MIN_VALUE, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaMultipleRecords() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"},
    //  {"event_id": 2, "occurred_at": "2018-10-27 00:38:13.005"}]
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZGFroGhroGpkr"
                    + "GBhYGRlYGRjpWRoYKNXqKKBoNSKk1djCytBYz8DAVKk2FgC35B+F");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    // we should output the oldest timestamp in the bundle
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaFutureOccurredAtTimestamp() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

    long loggedAtMillis = sdf.parse("2018-10-27 00:10:02.000000").getTime();
    String events =
        "[{\"event_id\": 1, \"occurred_at\": \"2018-10-27 00:20:02.900\", \"logged_at\": "
            + loggedAtMillis / 1000
            + "}]";
    byte[] message = encode(events);
    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(loggedAtMillis, value.getTimestamp().getMillis());
  }

  private static byte[] encode(String data) throws IOException {
    Deflater deflater = new Deflater();
    deflater.setInput(data.getBytes(Charset.defaultCharset()));
    deflater.finish();
    byte[] buf = new byte[4096];
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length())) {
      while (!deflater.finished()) {
        int count = deflater.deflate(buf);
        bos.write(buf, 0, count);
      }
      return bos.toByteArray();
    }
  }
}
