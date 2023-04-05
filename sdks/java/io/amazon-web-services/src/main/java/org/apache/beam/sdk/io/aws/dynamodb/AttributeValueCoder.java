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
package org.apache.beam.sdk.io.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A {@link Coder} that serializes and deserializes the {@link AttributeValue} objects. */
public class AttributeValueCoder extends AtomicCoder<AttributeValue> {

  /** Data type of each value type in AttributeValue object. */
  private enum AttributeValueType {
    s, // for String
    n, // for Number
    b, // for Byte
    sS, // for List of String
    nS, // for List of Number
    bS, // for List of Byte
    m, // for Map of String and AttributeValue
    l, // for list of AttributeValue
    bOOL, // for Boolean
    nULLValue, // for null
  }

  private static final AttributeValueCoder INSTANCE = new AttributeValueCoder();

  private static final ListCoder<String> LIST_STRING_CODER = ListCoder.of(StringUtf8Coder.of());
  private static final ListCoder<byte[]> LIST_BYTE_CODER = ListCoder.of(ByteArrayCoder.of());

  private static final ListCoder<AttributeValue> LIST_ATTRIBUTE_CODER =
      ListCoder.of(AttributeValueCoder.of());
  private static final MapCoder<String, AttributeValue> MAP_ATTRIBUTE_CODER =
      MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of());

  private AttributeValueCoder() {}

  public static AttributeValueCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(AttributeValue value, OutputStream outStream) throws IOException {

    if (value.getS() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.s.toString(), outStream);
      StringUtf8Coder.of().encode(value.getS(), outStream);
    } else if (value.getN() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.n.toString(), outStream);
      StringUtf8Coder.of().encode(value.getN(), outStream);
    } else if (value.getBOOL() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.bOOL.toString(), outStream);
      BooleanCoder.of().encode(value.getBOOL(), outStream);
    } else if (value.getB() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.b.toString(), outStream);
      ByteArrayCoder.of().encode(convertToByteArray(value.getB()), outStream);
    } else if (value.getSS() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.sS.toString(), outStream);
      LIST_STRING_CODER.encode(value.getSS(), outStream);
    } else if (value.getNS() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.nS.toString(), outStream);
      LIST_STRING_CODER.encode(value.getNS(), outStream);
    } else if (value.getBS() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.bS.toString(), outStream);
      LIST_BYTE_CODER.encode(convertToListByteArray(value.getBS()), outStream);
    } else if (value.getL() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.l.toString(), outStream);
      LIST_ATTRIBUTE_CODER.encode(value.getL(), outStream);
    } else if (value.getM() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.m.toString(), outStream);
      MAP_ATTRIBUTE_CODER.encode(value.getM(), outStream);
    } else if (value.getNULL() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.nULLValue.toString(), outStream);
      BooleanCoder.of().encode(value.getNULL(), outStream);
    } else {
      throw new CoderException("Unknown Type");
    }
  }

  @Override
  public AttributeValue decode(InputStream inStream) throws IOException {
    AttributeValue attrValue = new AttributeValue();

    String type = StringUtf8Coder.of().decode(inStream);
    AttributeValueType attrType = AttributeValueType.valueOf(type);

    switch (attrType) {
      case s:
        attrValue.setS(StringUtf8Coder.of().decode(inStream));
        break;
      case n:
        attrValue.setN(StringUtf8Coder.of().decode(inStream));
        break;
      case bOOL:
        attrValue.setBOOL(BooleanCoder.of().decode(inStream));
        break;
      case b:
        attrValue.setB(ByteBuffer.wrap(ByteArrayCoder.of().decode(inStream)));
        break;
      case sS:
        attrValue.setSS(LIST_STRING_CODER.decode(inStream));
        break;
      case nS:
        attrValue.setNS(LIST_STRING_CODER.decode(inStream));
        break;
      case bS:
        attrValue.setBS(convertToListByteBuffer(LIST_BYTE_CODER.decode(inStream)));
        break;
      case l:
        attrValue.setL(LIST_ATTRIBUTE_CODER.decode(inStream));
        break;
      case m:
        attrValue.setM(MAP_ATTRIBUTE_CODER.decode(inStream));
        break;
      case nULLValue:
        attrValue.setNULL(BooleanCoder.of().decode(inStream));
        break;
      default:
        throw new CoderException("Unknown Type");
    }

    return attrValue;
  }

  private List<byte[]> convertToListByteArray(List<ByteBuffer> listByteBuffer) {
    return listByteBuffer.stream().map(this::convertToByteArray).collect(Collectors.toList());
  }

  private byte[] convertToByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    buffer.position(buffer.position() - bytes.length);
    return bytes;
  }

  private List<ByteBuffer> convertToListByteBuffer(List<byte[]> listByteArr) {
    return listByteArr.stream().map(ByteBuffer::wrap).collect(Collectors.toList());
  }
}
