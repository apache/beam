/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for FlatMapFn.
 */
@RunWith(JUnit4.class)
public class FlatMapFnTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("serial")
  @Test
  public void testNoArgConstructorWithoutOverridingApplyThrowsIllegalStateException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("FlatMapFn#apply(InputT)");
    thrown.expectMessage("Didn't find");
    thrown.expectMessage("override");

    new FlatMapFn<String, Object>() {};
  }

  @SuppressWarnings("serial")
  @Test
  public void testNoArgConstructorWithDeclaredApplySucceeds() {
    new FlatMapFn<String, Object>() {
      @Override
      public Iterable<Object> apply(String input) {
        return null;
      }
    };
  }

  @SuppressWarnings("serial")
  @Test
  public void testNoArgConstructorWithMultipleDeclaredApplyWithDifferentErasureSucceeds() {
    new FlatMapFn<Integer, String>() {
      @Override
      public Iterable<String> apply(Integer input) {
        throw new IllegalArgumentException();
      }

      @SuppressWarnings("unused")
      public Iterable<String> apply(String input) {
        throw new IllegalStateException();
      }

    };
  }

  @SuppressWarnings("serial")
  @Test
  public void testSerializableFunctionWithNullThrowsNullPointerException() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("null SerializableFunction");
    thrown.expectMessage("FlatMapFn constructor");

    new FlatMapFn<String, String>(null) {};
  }

  @SuppressWarnings("serial")
  @Test
  public void testSerializableFunctionWithApplyOverrideThrowsIllegalStateException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("FlatMapFn#apply(InputT)");
    thrown.expectMessage("cannot be overriden");

    new FlatMapFn<String, String>(new SerializableFunction<String, Iterable<String>>() {
      @Override
      public Iterable<String> apply(String input) {
        return null;
      }
    }) {
      @Override
      public Iterable<String> apply(String input) {
        return null;
      }
    };
  }

  @SuppressWarnings("serial")
  @Test
  public void testSubclassNoArgConstructorWithDifferentTypeParametersSucceeds() {
    new ExtendedFlatMapFn<String, Object>() {
      @Override
      public Iterable<String> apply(Object input) {
        return null;
      }
    };
  }

  @SuppressWarnings("serial")
  private static class ExtendedFlatMapFn<OutputT, InputT> extends FlatMapFn<InputT, OutputT> {
    public ExtendedFlatMapFn() {
      super();
    }
  }
}

