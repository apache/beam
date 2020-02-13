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
package org.apache.beam.sdk.io.aws2.dynamodb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A {@link Coder} that serializes and deserializes the {@link AttributeValue} objects. */
public class AttributeValueCoder extends AtomicCoder<AttributeValue> {

  /** Data type of each value type in AttributeValue object. */
  private enum AttributeValueType {
    s, // for String
    n, // for Number
    b, // for Byte
    ss, // for List of String
    ns, // for List of Number
    bs, // for List of Byte
    m, // for Map of String and AttributeValue
    l, // for list of AttributeValue
    bool, // for Boolean
    nul, // for null
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
    if (value.s() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.s.toString(), outStream);
      StringUtf8Coder.of().encode(value.s(), outStream);
    } else if (value.n() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.n.toString(), outStream);
      StringUtf8Coder.of().encode(value.n(), outStream);
    } else if (value.bool() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.bool.toString(), outStream);
      BooleanCoder.of().encode(value.bool(), outStream);
    } else if (value.b() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.b.toString(), outStream);
      ByteArrayCoder.of().encode(value.b().asByteArray(), outStream);
    } else if (value.ss() != null && value.ss().size() > 0) {
      StringUtf8Coder.of().encode(AttributeValueType.ss.toString(), outStream);
      LIST_STRING_CODER.encode(value.ss(), outStream);
    } else if (value.ns() != null && value.ns().size() > 0) {
      StringUtf8Coder.of().encode(AttributeValueType.ns.toString(), outStream);
      LIST_STRING_CODER.encode(value.ns(), outStream);
    } else if (value.bs() != null && value.bs().size() > 0) {
      StringUtf8Coder.of().encode(AttributeValueType.bs.toString(), outStream);
      LIST_BYTE_CODER.encode(convertToListByteArray(value.bs()), outStream);
    } else if (value.l() != null && value.l().size() > 0) {
      StringUtf8Coder.of().encode(AttributeValueType.l.toString(), outStream);
      LIST_ATTRIBUTE_CODER.encode(value.l(), outStream);
    } else if (value.m() != null && value.m().size() > 0) {
      StringUtf8Coder.of().encode(AttributeValueType.m.toString(), outStream);
      MAP_ATTRIBUTE_CODER.encode(value.m(), outStream);
    } else if (value.nul() != null) {
      StringUtf8Coder.of().encode(AttributeValueType.nul.toString(), outStream);
      BooleanCoder.of().encode(value.nul(), outStream);
    } else {
      throw new CoderException("Unknown Type");
    }
  }

  @Override
  public AttributeValue decode(InputStream inStream) throws IOException {
    AttributeValue.Builder attrBuilder = AttributeValue.builder();

    String type = StringUtf8Coder.of().decode(inStream);
    AttributeValueType attrType = AttributeValueType.valueOf(type);

    switch (attrType) {
      case s:
        attrBuilder.s(StringUtf8Coder.of().decode(inStream));
        break;
      case n:
        attrBuilder.n(StringUtf8Coder.of().decode(inStream));
        break;
      case bool:
        attrBuilder.bool(BooleanCoder.of().decode(inStream));
        break;
      case b:
        attrBuilder.b(SdkBytes.fromByteArray(ByteArrayCoder.of().decode(inStream)));
        break;
      case ss:
        attrBuilder.ss(LIST_STRING_CODER.decode(inStream));
        break;
      case ns:
        attrBuilder.ns(LIST_STRING_CODER.decode(inStream));
        break;
      case bs:
        attrBuilder.bs((convertToListByteBuffer(LIST_BYTE_CODER.decode(inStream))));
        break;
      case l:
        attrBuilder.l(LIST_ATTRIBUTE_CODER.decode(inStream));
        break;
      case m:
        attrBuilder.m(MAP_ATTRIBUTE_CODER.decode(inStream));
        break;
      case nul:
        attrBuilder.nul(BooleanCoder.of().decode(inStream));
        break;
      default:
        throw new CoderException("Unknown Type");
    }

    return attrBuilder.build();
  }

  private List<byte[]> convertToListByteArray(List<SdkBytes> listSdkByte) {
    return listSdkByte.stream().map(SdkBytes::asByteArray).collect(Collectors.toList());
  }

  private List<SdkBytes> convertToListByteBuffer(List<byte[]> listByteArr) {
    return listByteArr.stream().map(SdkBytes::fromByteArray).collect(Collectors.toList());
  }
}
