/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.dataflow.sdk.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.dataflow.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import com.google.cloud.dataflow.sdk.util.state.StateContexts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;

/**
 * Unit tests for {@link CombineFnUtil}.
 */
@RunWith(JUnit4.class)
public class CombineFnUtilTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  KeyedCombineFnWithContext<Integer, Integer, Integer, Integer> mockCombineFn;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    mockCombineFn = mock(KeyedCombineFnWithContext.class, withSettings().serializable());
  }

  @Test
  public void testNonSerializable() throws Exception {
    expectedException.expect(NotSerializableException.class);
    expectedException.expectMessage(
        "Cannot serialize the CombineFn resulting from CombineFnUtil.bindContext.");

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(buffer);
    oos.writeObject(CombineFnUtil.bindContext(mockCombineFn, StateContexts.nullContext()));
  }
}
