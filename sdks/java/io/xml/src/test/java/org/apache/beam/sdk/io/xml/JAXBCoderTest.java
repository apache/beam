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
package org.apache.beam.sdk.io.xml;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JAXBCoder}. */
@RunWith(JUnit4.class)
public class JAXBCoderTest {

  @XmlRootElement
  static class TestType {
    private String testString = null;
    private int testInt;

    TestType() {}

    TestType(String testString, int testInt) {
      this.testString = testString;
      this.testInt = testInt;
    }

    public String getTestString() {
      return testString;
    }

    public void setTestString(String testString) {
      this.testString = testString;
    }

    public int getTestInt() {
      return testInt;
    }

    public void setTestInt(int testInt) {
      this.testInt = testInt;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;
      hashCode = 31 * hashCode + (testString == null ? 0 : testString.hashCode());
      hashCode = 31 * hashCode + testInt;
      return hashCode;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof TestType)) {
        return false;
      }

      TestType other = (TestType) obj;
      return (testString == null || testString.equals(other.testString))
          && (testInt == other.testInt);
    }
  }

  @Test
  public void testEncodeDecodeOuter() throws Exception {
    JAXBCoder<TestType> coder = JAXBCoder.of(TestType.class);

    byte[] encoded = CoderUtils.encodeToByteArray(coder, new TestType("abc", 9999));
    assertEquals(new TestType("abc", 9999), CoderUtils.decodeFromByteArray(coder, encoded));
  }

  @Test
  public void testEncodeDecodeAfterClone() throws Exception {
    JAXBCoder<TestType> coder = SerializableUtils.clone(JAXBCoder.of(TestType.class));

    byte[] encoded = CoderUtils.encodeToByteArray(coder, new TestType("abc", 9999));
    assertEquals(new TestType("abc", 9999), CoderUtils.decodeFromByteArray(coder, encoded));
  }

  @Test
  public void testEncodeDecodeNested() throws Exception {
    JAXBCoder<TestType> jaxbCoder = JAXBCoder.of(TestType.class);
    TestCoder nesting = new TestCoder(jaxbCoder);

    byte[] encoded = CoderUtils.encodeToByteArray(nesting, new TestType("abc", 9999));
    assertEquals(new TestType("abc", 9999), CoderUtils.decodeFromByteArray(nesting, encoded));
  }

  @Test
  @SuppressWarnings("AssertionFailureIgnored")
  public void testEncodeDecodeMultithreaded() throws Throwable {
    final JAXBCoder<TestType> coder = JAXBCoder.of(TestType.class);
    int numThreads = 100;

    final CountDownLatch ready = new CountDownLatch(numThreads);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(numThreads);

    final AtomicReference<Throwable> thrown = new AtomicReference<>();

    Executor executor = Executors.newCachedThreadPool();
    for (int i = 0; i < numThreads; i++) {
      final TestType elem = new TestType("abc", i);
      final int index = i;
      executor.execute(
          () -> {
            ready.countDown();
            try {
              start.await();
            } catch (InterruptedException ignored) {
            }

            try {
              byte[] encoded = CoderUtils.encodeToByteArray(coder, elem);
              assertEquals(
                  new TestType("abc", index), CoderUtils.decodeFromByteArray(coder, encoded));
            } catch (Throwable e) {
              thrown.compareAndSet(null, e);
            }
            done.countDown();
          });
    }
    ready.await();
    start.countDown();

    done.await();
    Throwable actuallyThrown = thrown.get();
    if (actuallyThrown != null) {
      throw actuallyThrown;
    }
  }

  /** A coder that surrounds the value with two values, to demonstrate nesting. */
  private static class TestCoder extends StructuredCoder<TestType> {
    private final JAXBCoder<TestType> jaxbCoder;

    TestCoder(JAXBCoder<TestType> jaxbCoder) {
      this.jaxbCoder = jaxbCoder;
    }

    @Override
    public void encode(TestType value, OutputStream outStream) throws IOException {
      encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(TestType value, OutputStream outStream, Context context) throws IOException {
      VarIntCoder.of().encode(3, outStream);
      jaxbCoder.encode(value, outStream);
      VarLongCoder.of().encode(22L, outStream, context);
    }

    @Override
    public TestType decode(InputStream inStream) throws IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public TestType decode(InputStream inStream, Context context) throws IOException {
      VarIntCoder.of().decode(inStream);
      TestType result = jaxbCoder.decode(inStream);
      VarLongCoder.of().decode(inStream, context);
      return result;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of(jaxbCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      jaxbCoder.verifyDeterministic();
    }
  }

  @Test
  public void testEncodable() {
    CoderProperties.coderSerializable(JAXBCoder.of(TestType.class));
  }

  @Test
  public void testEncodedTypeDescriptor() {
    assertThat(
        JAXBCoder.of(TestType.class).getEncodedTypeDescriptor(),
        equalTo(TypeDescriptor.of(TestType.class)));
  }
}
