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
package org.apache.beam.runners.apex.translation.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.datatorrent.lib.util.KryoCloneUtils;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.ValueState;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ApexStateInternals}. This is based on the tests for {@code StateInternalsTest}.
 */
public class ApexStateInternalsTest {

  private static StateInternals newStateInternals() {
    return new ApexStateInternals.ApexStateBackend()
        .newStateInternalsFactory(StringUtf8Coder.of())
        .stateInternalsForKey("dummyKey");
  }

  /** A standard StateInternals test. Ignore set and map tests. */
  @RunWith(JUnit4.class)
  public static class StandardStateInternalsTests extends StateInternalsTest {
    @Override
    protected StateInternals createStateInternals() {
      return newStateInternals();
    }

    @Override
    @Ignore
    public void testSet() {}

    @Override
    @Ignore
    public void testSetIsEmpty() {}

    @Override
    @Ignore
    public void testMergeSetIntoSource() {}

    @Override
    @Ignore
    public void testMergeSetIntoNewNamespace() {}

    @Override
    @Ignore
    public void testMap() {}

    @Override
    @Ignore
    public void testSetReadable() {}

    @Override
    @Ignore
    public void testMapReadable() {}
  }

  /** A specific test of ApexStateInternalsTest. */
  @RunWith(JUnit4.class)
  public static class OtherTests {

    private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");
    private static final StateTag<ValueState<String>> STRING_VALUE_ADDR =
        StateTags.value("stringValue", StringUtf8Coder.of());

    @Test
    public void testSerialization() throws Exception {
      ApexStateInternals.ApexStateInternalsFactory<String> sif =
          new ApexStateInternals.ApexStateBackend().newStateInternalsFactory(StringUtf8Coder.of());
      ApexStateInternals<String> keyAndState = sif.stateInternalsForKey("dummy");

      ValueState<String> value = keyAndState.state(NAMESPACE, STRING_VALUE_ADDR);
      assertEquals(keyAndState.state(NAMESPACE, STRING_VALUE_ADDR), value);
      value.write("hello");

      ApexStateInternals.ApexStateInternalsFactory<String> cloned;
      assertNotNull("Serialization", cloned = KryoCloneUtils.cloneObject(sif));
      ApexStateInternals<String> clonedKeyAndState = cloned.stateInternalsForKey("dummy");

      ValueState<String> clonedValue = clonedKeyAndState.state(NAMESPACE, STRING_VALUE_ADDR);
      assertThat(clonedValue.read(), Matchers.equalTo("hello"));
      assertEquals(clonedKeyAndState.state(NAMESPACE, STRING_VALUE_ADDR), value);
    }
  }
}
