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

import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Nested;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Primitive;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProtoDomain}. */
@RunWith(JUnit4.class)
public class ProtoDomainTest {

  @Test
  public void testNamespaceEqual() {
    ProtoDomain domainFromInt32 = ProtoDomain.buildFrom(Int32Value.getDescriptor());
    ProtoDomain domainFromInt64 = ProtoDomain.buildFrom(Int64Value.getDescriptor());
    Assert.assertTrue(domainFromInt64.equals(domainFromInt32));
  }

  @Test
  public void testContainsDescriptor() {
    ProtoDomain domainFromInt32 = ProtoDomain.buildFrom(Primitive.getDescriptor());
    Assert.assertTrue(domainFromInt32.contains(Primitive.getDescriptor()));
  }

  @Test
  public void testContainsOtherDescriptorSameFile() {
    ProtoDomain domain = ProtoDomain.buildFrom(Primitive.getDescriptor());
    Assert.assertTrue(domain.contains(Nested.getDescriptor()));
  }

  @Test
  public void testBuildForFile() {
    ProtoDomain domain = ProtoDomain.buildFrom(Primitive.getDescriptor().getFile());
    Assert.assertNotNull(domain.getFileDescriptor("google/protobuf/wrappers.proto"));
  }
}
