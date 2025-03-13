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
package org.apache.beam.sdk.extensions.protobuf;

import static org.apache.beam.sdk.testing.CoderProperties.ALL_CONTEXTS;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.DynamicMessage;
import java.io.ObjectStreamClass;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.Proto2CoderTestMessages.MessageA;
import org.apache.beam.sdk.extensions.protobuf.Proto2CoderTestMessages.MessageB;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProtoCoder}. */
@RunWith(JUnit4.class)
public class DynamicProtoCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDynamicMessage() throws Exception {
    DynamicMessage message =
        DynamicMessage.newBuilder(MessageA.getDescriptor())
            .setField(
                MessageA.getDescriptor().findFieldByNumber(MessageA.FIELD1_FIELD_NUMBER), "foo")
            .build();
    Coder<DynamicMessage> coder = DynamicProtoCoder.of(message.getDescriptorForType());

    // Special code to check the DynamicMessage equality (@see IsDynamicMessageEqual)
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.coderDecodeEncodeInContext(
          coder, context, message, IsDynamicMessageEqual.equalTo(message));
    }
  }

  @Test
  public void testDynamicNestedRepeatedMessage() throws Exception {
    DynamicMessage message =
        DynamicMessage.newBuilder(MessageA.getDescriptor())
            .setField(
                MessageA.getDescriptor().findFieldByNumber(MessageA.FIELD1_FIELD_NUMBER), "foo")
            .addRepeatedField(
                MessageA.getDescriptor().findFieldByNumber(MessageA.FIELD2_FIELD_NUMBER),
                DynamicMessage.newBuilder(MessageB.getDescriptor())
                    .setField(
                        MessageB.getDescriptor().findFieldByNumber(MessageB.FIELD1_FIELD_NUMBER),
                        true)
                    .build())
            .addRepeatedField(
                MessageA.getDescriptor().findFieldByNumber(MessageA.FIELD2_FIELD_NUMBER),
                DynamicMessage.newBuilder(MessageB.getDescriptor())
                    .setField(
                        MessageB.getDescriptor().findFieldByNumber(MessageB.FIELD1_FIELD_NUMBER),
                        false)
                    .build())
            .build();
    Coder<DynamicMessage> coder = DynamicProtoCoder.of(message.getDescriptorForType());

    // Special code to check the DynamicMessage equality (@see IsDynamicMessageEqual)
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.coderDecodeEncodeInContext(
          coder, context, message, IsDynamicMessageEqual.equalTo(message));
    }
  }

  @Test
  public void testSerialVersionID() {
    long serialVersionID = ObjectStreamClass.lookup(DynamicProtoCoder.class).getSerialVersionUID();
    assertEquals(1L, serialVersionID);
  }
}
