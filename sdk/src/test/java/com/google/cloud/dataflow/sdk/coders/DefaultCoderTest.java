/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.google.api.client.util.Preconditions;
import com.google.common.reflect.TypeToken;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests of Coder defaults.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class DefaultCoderTest {

  @DefaultCoder(AvroCoder.class)
  private static class AvroRecord {
  }

  private static class SerializableBase implements Serializable {
  }

  @DefaultCoder(SerializableCoder.class)
  private static class SerializableRecord extends SerializableBase {
  }

  @DefaultCoder(CustomCoder.class)
  private static class CustomRecord extends SerializableBase {
  }

  private static class Unknown {
  }

  private static class CustomCoder extends SerializableCoder<CustomRecord> {
    // Extending SerializableCoder isn't trivial, but it can be done.
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableCoder<T> of(Class<T> recordType) {
       Preconditions.checkArgument(
           CustomRecord.class.isAssignableFrom(recordType));
       return (SerializableCoder<T>) new CustomCoder();
    }

    protected CustomCoder() {
      super(CustomRecord.class);
    }
  }

  @Test
  public void testDefaultCoders() throws Exception {
    checkDefault(AvroRecord.class, AvroCoder.class);
    checkDefault(SerializableBase.class, SerializableCoder.class);
    checkDefault(SerializableRecord.class, SerializableCoder.class);
    checkDefault(CustomRecord.class, CustomCoder.class);
  }

  @Test
  public void testUnknown() throws Exception {
    CoderRegistry registery = new CoderRegistry();
    Coder<?> coderType = registery.getDefaultCoder(Unknown.class);
    Assert.assertNull(coderType);
  }

  /**
   * Checks that the default Coder for {@code valueType} is an instance of
   * {@code expectedCoder}.
   */
  private void checkDefault(Class<?> valueType,
      Class<?> expectedCoder) {
    CoderRegistry registry = new CoderRegistry();
    Coder<?> coder = registry.getDefaultCoder(TypeToken.of(valueType));
    Assert.assertThat(coder, Matchers.instanceOf(expectedCoder));
  }
}
