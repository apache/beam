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
package org.apache.beam.sdk.extensions.avro.io;

import static org.apache.avro.file.DataFileConstants.BZIP2_CODEC;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.file.DataFileConstants.NULL_CODEC;
import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.apache.avro.file.DataFileConstants.XZ_CODEC;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of SerializableAvroCodecFactory. */
@RunWith(JUnit4.class)
public class SerializableAvroCodecFactoryTest {
  private static final String VERSION_AVRO =
      org.apache.avro.Schema.class.getPackage().getImplementationVersion();

  private static final List<String> avroCodecs = new ArrayList<>();

  static {
    avroCodecs.addAll(
        Arrays.asList(NULL_CODEC, SNAPPY_CODEC, DEFLATE_CODEC, XZ_CODEC, BZIP2_CODEC));

    // Zstd codec not available until Avro 1.9
    if (!VERSION_AVRO.startsWith("1.8.")) {
      avroCodecs.add("zstandard");
    }
  }

  @Test
  public void testDefaultCodecsIn() throws Exception {
    for (String codec : avroCodecs) {
      SerializableAvroCodecFactory codecFactory =
          new SerializableAvroCodecFactory(CodecFactory.fromString(codec));

      assertEquals(CodecFactory.fromString(codec).toString(), codecFactory.getCodec().toString());
    }
  }

  @Test
  public void testDefaultCodecsSerDe() throws Exception {
    for (String codec : avroCodecs) {
      SerializableAvroCodecFactory codecFactory =
          new SerializableAvroCodecFactory(CodecFactory.fromString(codec));

      SerializableAvroCodecFactory serdeC = SerializableUtils.clone(codecFactory);

      assertEquals(CodecFactory.fromString(codec).toString(), serdeC.getCodec().toString());
    }
  }

  @Test
  public void testDeflateCodecSerDeWithLevels() throws Exception {
    for (int i = 0; i < 10; ++i) {
      SerializableAvroCodecFactory codecFactory =
          new SerializableAvroCodecFactory(CodecFactory.deflateCodec(i));

      SerializableAvroCodecFactory serdeC = SerializableUtils.clone(codecFactory);

      assertEquals(CodecFactory.deflateCodec(i).toString(), serdeC.getCodec().toString());
    }
  }

  @Test
  public void testXZCodecSerDeWithLevels() throws Exception {
    for (int i = 0; i < 10; ++i) {
      SerializableAvroCodecFactory codecFactory =
          new SerializableAvroCodecFactory(CodecFactory.xzCodec(i));

      SerializableAvroCodecFactory serdeC = SerializableUtils.clone(codecFactory);

      assertEquals(CodecFactory.xzCodec(i).toString(), serdeC.getCodec().toString());
    }
  }

  @Test
  public void testZstdCodecSerDeWithLevels() throws Exception {
    if (VERSION_AVRO.startsWith("1.8.")) {
      // Skip, zstd only supported for Avro 1.9+
      return;
    }

    for (int i = -7; i <= 22; i++) {
      SerializableAvroCodecFactory codecFactory = new SerializableAvroCodecFactory();

      // Deserialize a ZStandardCodec instance from bytes; we can't reference the class directly
      // since it won't compile for Avro 1.8
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final ObjectOutputStream os = new ObjectOutputStream(baos);
      os.writeUTF("zstandard[" + i + "]");
      os.flush();
      codecFactory.readExternal(
          new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));

      assertEquals("zstandard[" + i + "]", codecFactory.getCodec().toString());

      // Test cloning behavior
      SerializableAvroCodecFactory clone = SerializableUtils.clone(codecFactory);
      assertEquals(codecFactory.getCodec().toString(), clone.getCodec().toString());
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullCodecToString() throws Exception {
    // use default CTR (available cause Serializable)
    SerializableAvroCodecFactory codec = new SerializableAvroCodecFactory();
    assertEquals("null", codec.toString());
  }
}
