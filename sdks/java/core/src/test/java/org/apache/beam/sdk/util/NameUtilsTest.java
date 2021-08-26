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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.NameUtils.NameOverride;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NameUtils}. */
@RunWith(JUnit4.class)
public class NameUtilsTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDropsStandardSuffixes() {
    assertEquals("Embedded", NameUtils.approximateSimpleName("EmbeddedDoFn", true));
    assertEquals("Embedded", NameUtils.approximateSimpleName("EmbeddedFn", true));

    assertEquals("Embedded", NameUtils.approximateSimpleName("EmbeddedDoFn", false));
    assertEquals("Embedded", NameUtils.approximateSimpleName("EmbeddedFn", false));
  }

  @Test
  public void testDropsStandardSuffixesInAllComponents() {
    assertEquals("Embedded", NameUtils.approximateSimpleName("SomeDoFn$EmbeddedDoFn", true));
    assertEquals("Embedded", NameUtils.approximateSimpleName("SomeFn$EmbeddedFn", true));

    assertEquals("Some.Embedded", NameUtils.approximateSimpleName("SomeDoFn$EmbeddedDoFn", false));
    assertEquals("Some.Embedded", NameUtils.approximateSimpleName("SomeFn$EmbeddedFn", false));
  }

  @Test
  public void testDropsOuterClassNamesTrue() {
    assertEquals("Bar", NameUtils.approximateSimpleName("Foo$1$Bar", true));
    assertEquals("Foo$1", NameUtils.approximateSimpleName("Foo$1", true));
    assertEquals("Foo$1$2", NameUtils.approximateSimpleName("Foo$1$2", true));
  }

  @Test
  public void testDropsOuterClassNamesFalse() {
    assertEquals("Foo.Bar", NameUtils.approximateSimpleName("Foo$1$Bar", false));
    assertEquals("Foo.1", NameUtils.approximateSimpleName("Foo$1", false));
    assertEquals("Foo.2", NameUtils.approximateSimpleName("Foo$1$2", false));
  }

  /** Inner class for simple name test. */
  @SuppressWarnings("ClassCanBeStatic")
  private class EmbeddedDoFn {

    private class DeeperEmbeddedDoFn extends EmbeddedDoFn {}

    private EmbeddedDoFn getEmbedded() {
      return new DeeperEmbeddedDoFn();
    }
  }

  private static class EmbeddedPTransform extends PTransform<PBegin, PDone> {
    @Override
    public PDone expand(PBegin begin) {
      throw new IllegalArgumentException("Should never be applied");
    }

    @SuppressWarnings("ClassCanBeStatic")
    private class Bound extends PTransform<PBegin, PDone> {
      @Override
      public PDone expand(PBegin begin) {
        throw new IllegalArgumentException("Should never be applied");
      }
    }

    private Bound getBound() {
      return new Bound();
    }
  }

  private interface AnonymousClass {
    Object getInnerClassInstance();
  }

  @Test
  public void testSimpleName() {
    assertEquals("Embedded", NameUtils.approximateSimpleName(new EmbeddedDoFn()));
  }

  @Test
  public void testAnonSimpleName() throws Exception {
    assertEquals("Anonymous", NameUtils.approximateSimpleName(new EmbeddedDoFn() {}));
  }

  @Test
  public void testNestedSimpleName() {
    EmbeddedDoFn fn = new EmbeddedDoFn();
    EmbeddedDoFn inner = fn.getEmbedded();

    assertEquals("DeeperEmbedded", NameUtils.approximateSimpleName(inner));
  }

  @Test
  public void testPTransformName() {
    EmbeddedPTransform transform = new EmbeddedPTransform();
    assertEquals(
        "NameUtilsTest.EmbeddedPTransform",
        NameUtils.approximatePTransformName(transform.getClass()));
    assertEquals(
        "NameUtilsTest.EmbeddedPTransform",
        NameUtils.approximatePTransformName(transform.getBound().getClass()));
    assertEquals(
        "NameUtilsTest.SomeTransform",
        NameUtils.approximatePTransformName(AutoValue_NameUtilsTest_SomeTransform.class));
    assertEquals("TextIO.Write", NameUtils.approximatePTransformName(TextIO.Write.class));
  }

  @AutoValue
  abstract static class SomeTransform extends PTransform<PBegin, PDone> {
    @Override
    public PDone expand(PBegin input) {
      return null;
    }
  }

  @Test
  public void testPTransformNameWithAnonOuterClass() throws Exception {
    AnonymousClass anonymousClassObj =
        new AnonymousClass() {
          class NamedInnerClass extends PTransform<PBegin, PDone> {
            @Override
            public PDone expand(PBegin begin) {
              throw new IllegalArgumentException("Should never be applied");
            }
          }

          @Override
          public Object getInnerClassInstance() {
            return new NamedInnerClass();
          }
        };

    assertEquals(
        "NamedInnerClass",
        NameUtils.approximateSimpleName(anonymousClassObj.getInnerClassInstance()));
    assertEquals(
        "NameUtilsTest.NamedInnerClass",
        NameUtils.approximatePTransformName(anonymousClassObj.getInnerClassInstance().getClass()));
  }

  @Test
  public void testApproximateSimpleNameOverride() {
    NameOverride overriddenName = () -> "CUSTOM_NAME";
    assertEquals("CUSTOM_NAME", NameUtils.approximateSimpleName(overriddenName));
  }

  @Test
  public void testApproximateSimpleNameCustomAnonymous() {
    Object overriddenName = new Object() {};
    assertEquals("CUSTOM_NAME", NameUtils.approximateSimpleName(overriddenName, "CUSTOM_NAME"));
  }
}
