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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * Is the DynamicMessage equal to another message. This special matcher exist because the
 * DynamicMessage is protobuf does a object equality in it's equals operator.
 *
 * <p>Follow https://github.com/protocolbuffers/protobuf/issues/6100 for tracking the issue. If this
 * is resolved we can remove this code.
 */
public class IsDynamicMessageEqual extends BaseMatcher<DynamicMessage> {
  private final DynamicMessage expectedValue;

  public IsDynamicMessageEqual(DynamicMessage equalArg) {
    expectedValue = equalArg;
  }

  public static IsDynamicMessageEqual equalTo(DynamicMessage operand) {
    return new IsDynamicMessageEqual(operand);
  }

  @Override
  public boolean matches(Object actualValue) {

    if (actualValue == null) {
      return expectedValue == null;
    }

    if (!(actualValue instanceof Message)) {
      return false;
    }
    final Message actualMessage = (Message) actualValue;

    if (!actualMessage.toByteString().equals(expectedValue.toByteString())) {
      return false;
    }

    return actualMessage
        .getDescriptorForType()
        .getFullName()
        .equals(expectedValue.getDescriptorForType().getFullName());
  }

  @Override
  public void describeTo(Description description) {
    description.appendValue(expectedValue);
  }
}
