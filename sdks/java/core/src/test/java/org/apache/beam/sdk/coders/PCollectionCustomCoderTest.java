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

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for coder exception handling in runners. */
@RunWith(JUnit4.class)
public class PCollectionCustomCoderTest {
  /**
   * A custom test coder that can throw various exceptions during:
   *
   * <ul>
   *   <li>encoding or decoding of data.
   *   <li>serializing or deserializing the coder.
   * </ul>
   */
  static final String IO_EXCEPTION = "java.io.IOException";

  static final String NULL_POINTER_EXCEPTION = "java.lang.NullPointerException";
  static final String EXCEPTION_MESSAGE = "Super Unique Message!!!";

  @Rule public ExpectedException thrown = ExpectedException.none();

  /** Wrapper of StringUtf8Coder with customizable exception-throwing. */
  public static class CustomTestCoder extends CustomCoder<String> {
    private final String decodingException;
    private final String encodingException;
    private final String serializationException;
    private final String deserializationException;
    private final String exceptionMessage;

    /**
     * @param decodingException The fully qualified class name of the exception to throw while
     *     decoding data. e.g. (e.g. 'java.lang.NullPointerException')
     * @param encodingException The fully qualified class name of the exception to throw while
     *     encoding data. e.g. (e.g. 'java.lang.NullPointerException')
     * @param serializationException The fully qualified class name of the exception to throw while
     *     serializing the coder. (e.g. 'java.lang.NullPointerException')
     * @param deserializationException The fully qualified class name of the exception to throw
     *     while deserializing the coder. (e.g. 'java.lang.NullPointerException')
     * @param exceptionMessage The message to place inside the body of the exception.
     */
    public CustomTestCoder(
        String decodingException,
        String encodingException,
        String serializationException,
        String deserializationException,
        String exceptionMessage) {
      this.decodingException = decodingException;
      this.encodingException = encodingException;
      this.serializationException = serializationException;
      this.deserializationException = deserializationException;
      this.exceptionMessage = exceptionMessage;
    }

    @Override
    public void encode(String value, OutputStream outStream) throws CoderException, IOException {
      throwIfPresent(encodingException);
      StringUtf8Coder.of().encode(value, outStream);
    }

    @Override
    public String decode(InputStream inStream) throws CoderException, IOException {
      throwIfPresent(decodingException);
      return StringUtf8Coder.of().decode(inStream);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      throwIfPresent(serializationException);
      out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      throwIfPresent(deserializationException);
    }

    /**
     * Constructs and throws the specified exception.
     *
     * @param exceptionClassName The fully qualified class name of the exception to throw. If null,
     *     then no exception will be thrown.
     * @throws IOException Is thrown if java.lang.IOException is passed in as exceptionClassName.
     */
    private void throwIfPresent(String exceptionClassName) throws IOException {
      if (exceptionClassName == null) {
        return;
      }
      try {
        Object throwable =
            Class.forName(exceptionClassName)
                .getConstructor(String.class)
                .newInstance(exceptionMessage);
        if (throwable instanceof IOException) {
          throw (IOException) throwable;
        } else {
          throw new RuntimeException((Throwable) throwable);
        }
      } catch (InvocationTargetException
          | NoSuchMethodException
          | ClassNotFoundException
          | IllegalAccessException
          | InstantiationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDecodingIOException() throws Exception {
    thrown.expect(Exception.class);
    thrown.expectCause(instanceOf(IOException.class));
    Pipeline p =
        runPipelineWith(new CustomTestCoder(IO_EXCEPTION, null, null, null, EXCEPTION_MESSAGE));

    p.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDecodingNPException() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("java.lang.NullPointerException: Super Unique Message!!!");

    Pipeline p =
        runPipelineWith(
            new CustomTestCoder(NULL_POINTER_EXCEPTION, null, null, null, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEncodingIOException() throws Exception {
    thrown.expect(Exception.class);
    thrown.expectCause(instanceOf(IOException.class));

    Pipeline p =
        runPipelineWith(new CustomTestCoder(null, IO_EXCEPTION, null, null, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEncodingNPException() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("java.lang.NullPointerException: Super Unique Message!!!");
    Pipeline p =
        runPipelineWith(
            new CustomTestCoder(null, NULL_POINTER_EXCEPTION, null, null, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSerializationIOException() throws Exception {
    thrown.expect(Exception.class);
    thrown.expectCause(instanceOf(IOException.class));
    Pipeline p =
        runPipelineWith(new CustomTestCoder(null, null, IO_EXCEPTION, null, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSerializationNPException() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("java.lang.NullPointerException: Super Unique Message!!!");

    Pipeline p =
        runPipelineWith(
            new CustomTestCoder(null, null, NULL_POINTER_EXCEPTION, null, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDeserializationIOException() throws Exception {
    thrown.expect(Exception.class);
    thrown.expectCause(instanceOf(IOException.class));
    Pipeline p =
        runPipelineWith(new CustomTestCoder(null, null, null, IO_EXCEPTION, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDeserializationNPException() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("java.lang.NullPointerException: Super Unique Message!!!");

    Pipeline p =
        runPipelineWith(
            new CustomTestCoder(null, null, null, NULL_POINTER_EXCEPTION, EXCEPTION_MESSAGE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNoException() throws Exception {
    Pipeline p = runPipelineWith(new CustomTestCoder(null, null, null, null, null));
  }

  public static Pipeline runPipelineWith(CustomTestCoder coder) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs().as(PipelineOptions.class);

    List<String> pipelineContents =
        Arrays.asList("String", "Testing", "Custom", "Coder", "In", "Beam");

    // Create input.
    Pipeline pipeline = TestPipeline.create(options);
    PCollection<String> customCoderPC =
        pipeline.begin().apply("ReadStrings", Create.of(pipelineContents)).setCoder(coder);
    PAssert.that(customCoderPC).containsInAnyOrder(pipelineContents);
    pipeline.run().waitUntilFinish();

    return pipeline;
  }
}
