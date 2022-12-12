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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoByteBuddyUtilsTest {

  private static final String PROTO_PROPERTY_WITH_UNDERSCORE = "foo_bar_id";
  private static final String PROTO_PROPERTY_WITH_NUMBER = "foo2bar_id";

  private static final String JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_UNDERSCORE = "FooBarId";
  private static final String JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_NUMBER = "Foo2BarId";

  @Test
  public void testGetterNameCreationForProtoPropertyWithUnderscore() {
    Assert.assertEquals(
        JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_UNDERSCORE,
        ProtoByteBuddyUtils.convertProtoPropertyNameToJavaPropertyName(
            PROTO_PROPERTY_WITH_UNDERSCORE));
  }

  @Test
  public void testGetterNameCreationForProtoPropertyWithNumber() {
    Assert.assertEquals(
        JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_NUMBER,
        ProtoByteBuddyUtils.convertProtoPropertyNameToJavaPropertyName(PROTO_PROPERTY_WITH_NUMBER));
  }

  @Test
  public void testGetterExistenceForProtoPropertyWithUnderscore() {
    try {
      Assert.assertNotNull(
          ProtoByteBuddyUtilsMessages.ProtoByteBuddyUtilsMessageWithUnderscore.class.getMethod(
              "get" + JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_UNDERSCORE));
    } catch (NoSuchMethodException e) {
      Assert.fail(
          "Unable to find expected getter method for "
              + PROTO_PROPERTY_WITH_UNDERSCORE
              + " -> "
              + e);
    }
  }

  @Test
  public void testGetterExistenceForProtoPropertyWithNumber() {
    try {
      Assert.assertNotNull(
          ProtoByteBuddyUtilsMessages.ProtoByteBuddyUtilsMessageWithNumber.class.getMethod(
              "get" + JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_NUMBER));
    } catch (NoSuchMethodException e) {
      Assert.fail(
          "Unable to find expected getter method for "
              + JAVA_PROPERTY_FOR_PROTO_PROPERTY_WITH_NUMBER
              + " -> "
              + e);
    }
  }
}
