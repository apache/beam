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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link StructuredCoder}. */
@RunWith(JUnit4.class)
public class StructuredCoderTest {

  /** A coder for nullable {@code Boolean} values that is consistent with equals. */
  private static class NullBooleanCoder extends StructuredCoder<Boolean> {

    private static final long serialVersionUID = 0L;

    @Override
    public void encode(@Nullable Boolean value, OutputStream outStream)
        throws CoderException, IOException {
      if (value == null) {
        outStream.write(2);
      } else if (value) {
        outStream.write(1);
      } else {
        outStream.write(0);
      }
    }

    @Override
    public @Nullable Boolean decode(InputStream inStream) throws CoderException, IOException {
      int value = inStream.read();
      if (value == 0) {
        return false;
      } else if (value == 1) {
        return true;
      } else if (value == 2) {
        return null;
      }
      throw new CoderException("Invalid value for nullable Boolean: " + value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }
  }

  /** A boxed {@code int} with {@code equals()} that compares object identity. */
  private static class ObjectIdentityBoolean {
    private final boolean value;

    public ObjectIdentityBoolean(boolean value) {
      this.value = value;
    }

    public boolean getValue() {
      return value;
    }
  }

  /** A coder for nullable boxed {@code Boolean} values that is not consistent with equals. */
  private static class ObjectIdentityBooleanCoder extends StructuredCoder<ObjectIdentityBoolean> {

    private static final long serialVersionUID = 0L;

    @Override
    public void encode(@Nullable ObjectIdentityBoolean value, OutputStream outStream)
        throws CoderException, IOException {
      if (value == null) {
        outStream.write(2);
      } else if (value.getValue()) {
        outStream.write(1);
      } else {
        outStream.write(0);
      }
    }

    @Override
    public @Nullable ObjectIdentityBoolean decode(InputStream inStream)
        throws CoderException, IOException {
      int value = inStream.read();
      if (value == 0) {
        return new ObjectIdentityBoolean(false);
      } else if (value == 1) {
        return new ObjectIdentityBoolean(true);
      } else if (value == 2) {
        return null;
      }
      throw new CoderException("Invalid value for nullable Boolean: " + value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public boolean consistentWithEquals() {
      return false;
    }
  }

  /**
   * Tests that {@link StructuredCoder#structuralValue()} is correct whenever a subclass has a
   * correct {@link Coder#consistentWithEquals()}.
   */
  @Test
  public void testStructuralValue() throws Exception {
    List<Boolean> testBooleans = Arrays.asList(null, true, false);
    List<ObjectIdentityBoolean> testInconsistentBooleans =
        Arrays.asList(null, new ObjectIdentityBoolean(true), new ObjectIdentityBoolean(false));

    Coder<Boolean> consistentCoder = new NullBooleanCoder();
    for (Boolean value1 : testBooleans) {
      for (Boolean value2 : testBooleans) {
        CoderProperties.structuralValueConsistentWithEquals(consistentCoder, value1, value2);
      }
    }

    Coder<ObjectIdentityBoolean> inconsistentCoder = new ObjectIdentityBooleanCoder();
    for (ObjectIdentityBoolean value1 : testInconsistentBooleans) {
      for (ObjectIdentityBoolean value2 : testInconsistentBooleans) {
        CoderProperties.structuralValueConsistentWithEquals(inconsistentCoder, value1, value2);
      }
    }
  }

  /** Test for verifying {@link StructuredCoder#toString()}. */
  @Test
  public void testToString() {
    Assert.assertThat(
        new ObjectIdentityBooleanCoder().toString(),
        CoreMatchers.equalTo("StructuredCoderTest$ObjectIdentityBooleanCoder"));

    ObjectIdentityBooleanCoder coderWithArgs =
        new ObjectIdentityBooleanCoder() {
          @Override
          public List<? extends Coder<?>> getCoderArguments() {
            return ImmutableList.<Coder<?>>builder()
                .add(BigDecimalCoder.of(), BigIntegerCoder.of())
                .build();
          }
        };

    Assert.assertThat(
        coderWithArgs.toString(),
        CoreMatchers.equalTo("StructuredCoderTest$1(BigDecimalCoder,BigIntegerCoder)"));
  }

  @Test
  public void testGenericStandardCoderFallsBackToT() throws Exception {
    Assert.assertThat(
        new Foo<String>().getEncodedTypeDescriptor().getType(),
        CoreMatchers.not(TypeDescriptor.of(String.class).getType()));
  }

  @Test
  public void testGenericStandardCoder() throws Exception {
    Assert.assertThat(
        new FooTwo().getEncodedTypeDescriptor(),
        CoreMatchers.equalTo(TypeDescriptor.of(String.class)));
  }

  private static class Foo<T> extends StructuredCoder<T> {

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {}
  }

  private static class FooTwo extends Foo<String> {}
}
