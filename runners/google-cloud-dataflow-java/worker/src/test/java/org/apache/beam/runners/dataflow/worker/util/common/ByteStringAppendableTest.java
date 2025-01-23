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
package org.apache.beam.runners.dataflow.worker.util.common;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStringAppendableTest {

  @Test
  public void equivalentToOutputStreamWriter() throws IOException {
    String randomString = "⣏⓫⦎⊺ⱄ\u243B♼⢓␜\u2065✝⡳oⶤⱲ⨻1⅒ↀ◅⡪⋲";
    ByteString byteString1, byteString2;
    {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
      writer.append(randomString);
      writer.flush();
      byteString1 = stream.toByteString();
    }

    {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      ByteStringAppendable appendable = new ByteStringAppendable(stream);
      appendable.append(randomString);
      byteString2 = stream.toByteString();
    }
    assertEquals(byteString1, byteString2);
  }

  @Test
  public void equivalentToOutputStreamWriterSubstr() throws IOException {
    String randomString = "⣏⓫⦎⊺ⱄ\u243B♼⢓␜\u2065✝⡳oⶤⱲ⨻1⅒ↀ◅⡪⋲";
    ByteString byteString1, byteString2;
    {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
      writer.append(randomString, 3, 10);
      writer.flush();
      byteString1 = stream.toByteString();
    }

    {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      ByteStringAppendable appendable = new ByteStringAppendable(stream);
      appendable.append(randomString, 3, 10);
      byteString2 = stream.toByteString();
    }
    assertEquals(byteString1, byteString2);
  }

  @Test
  public void equivalentToOutputStreamWriterChar() throws IOException {
    for (char c=0; c<=255; ++c) {
      ByteString byteString1, byteString2;
      {
        ByteStringOutputStream stream = new ByteStringOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
        writer.append(c);
        writer.flush();
        byteString1 = stream.toByteString();
      }

      {
        ByteStringOutputStream stream = new ByteStringOutputStream();
        ByteStringAppendable appendable = new ByteStringAppendable(stream);
        appendable.append(c);
        byteString2 = stream.toByteString();
      }
      assertEquals(byteString1, byteString2);
    }
  }
}
