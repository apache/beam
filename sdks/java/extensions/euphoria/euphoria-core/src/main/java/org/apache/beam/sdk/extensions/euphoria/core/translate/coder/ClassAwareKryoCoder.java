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
package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.VoidFunction;
import org.apache.commons.compress.utils.BoundedInputStream;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * {@link Coder} which encodes/decodes objects of given {@link Class} through {@link Kryo}.
 */
public class ClassAwareKryoCoder<T> extends Coder<T> {

  /**
   * Initial size of byte buffers in {@link Output}, {@link Input}.
   */
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  // this factory needs to be serializable, since the Coder itself is
  private static final VoidFunction<Kryo> KRYO_FACTORY =
      () -> {
        final Kryo instance = new Kryo();
        ((Kryo.DefaultInstantiatorStrategy) instance.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        return instance;
      };

  private static ThreadLocal<Kryo> threadLocalKryo = ThreadLocal.withInitial(KRYO_FACTORY::apply);

  private static ThreadLocal<Output> threadLocalOutput = ThreadLocal.withInitial(
      () -> new Output(DEFAULT_BUFFER_SIZE, -1)
  );
  private static ThreadLocal<Input> threadLocalInput = ThreadLocal.withInitial(
      () -> new Input(DEFAULT_BUFFER_SIZE)
  );

  private Class<T> clazz;

  public ClassAwareKryoCoder(Class<T> clazz) {
    Objects.requireNonNull(clazz);
    this.clazz = clazz;
  }

  public static <T> ClassAwareKryoCoder<T> of(Class<T> clazz) {
    return new ClassAwareKryoCoder<>(clazz);
  }

  @Override
  public synchronized void encode(T value, OutputStream outStream)
      throws CoderException, IOException {

    Output output = threadLocalOutput.get();
    output.clear();
    threadLocalKryo.get().writeObjectOrNull(output, value, clazz);

    DataOutputStream dos = new DataOutputStream(outStream);
    // write length of encoded object first to get ability to limit read in decode() to one object
    dos.writeInt(output.position());
    outStream.write(output.getBuffer(), 0, output.position());

  }

  @Override
  public synchronized T decode(InputStream inStream) throws CoderException, IOException {
    DataInputStream dis = new DataInputStream(inStream);
    int lengthOfDecodedObject = dis.readInt();

    Input input = threadLocalInput.get();

    BoundedInputStream limitedStream = new BoundedInputStream(inStream, lengthOfDecodedObject);
    input.setInputStream(limitedStream);

    return threadLocalKryo.get().readObjectOrNull(input, clazz);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  } //TODO Is there a way of knowing this ?

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    //no op
  }

  public Class<T> getClazz() {
    return clazz;
  }

}
