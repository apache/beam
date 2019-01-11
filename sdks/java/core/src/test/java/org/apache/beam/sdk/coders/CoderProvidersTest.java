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
package org.apache.beam.sdk.coders;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.Map;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CoderFactories}.
 */
@RunWith(JUnit4.class)
public class CoderProvidersTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAvroThenSerializableStringMap() throws Exception {
    CoderProvider provider = CoderProviders.firstOf(AvroCoder.PROVIDER, SerializableCoder.PROVIDER);
    Coder<Map<String, String>> coder =
        provider.getCoder(new TypeDescriptor<Map<String, String>>(){});
    assertThat(coder, instanceOf(AvroCoder.class));
  }

  @Test
  public void testThrowingThenSerializable() throws Exception {
    CoderProvider provider =
        CoderProviders.firstOf(new ThrowingCoderProvider(), SerializableCoder.PROVIDER);
    Coder<Integer> coder = provider.getCoder(new TypeDescriptor<Integer>(){});
    assertThat(coder, instanceOf(SerializableCoder.class));
  }

  @Test
  public void testNullThrows() throws Exception {
    CoderProvider provider = CoderProviders.firstOf(new ThrowingCoderProvider());
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage("ThrowingCoderProvider");
    provider.getCoder(new TypeDescriptor<Integer>(){});
  }

  private static class ThrowingCoderProvider implements CoderProvider {
    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
      throw new CannotProvideCoderException("ThrowingCoderProvider cannot ever provide a Coder");
    }
  }
}
