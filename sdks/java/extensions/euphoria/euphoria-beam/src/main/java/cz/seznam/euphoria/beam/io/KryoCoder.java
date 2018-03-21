/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import cz.seznam.euphoria.core.client.functional.VoidFunction;
import org.apache.beam.sdk.coders.CustomCoder;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coder using Kryo as (de)serialization mechanism.
 * FIXME: we should entirely drop this class
 */
public class KryoCoder<T> extends CustomCoder<T> {

  public static VoidFunction<Kryo> FACTORY = () -> {
    final Kryo instance = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) instance.getInstantiatorStrategy())
        .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    return instance;
  };

  public static void withKryoFactory(VoidFunction<Kryo> factory) {
    FACTORY = factory;
    kryo = ThreadLocal.withInitial(FACTORY::apply);
  }

  private static AtomicBoolean reinitialize = new AtomicBoolean(true);
  private static ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(FACTORY::apply);

  // factory that need to be serialized and deserialized
  // use the current static factory, that has been set
  private final VoidFunction<Kryo> factory = FACTORY;

  @Override
  public void encode(T t, OutputStream out) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Output output = new Output(baos);
    kryo.get().writeClassAndObject(output, t);
    output.flush();
    final DataOutputStream dos = new DataOutputStream(out);
    dos.writeInt(baos.toByteArray().length);
    dos.write(baos.toByteArray());
  }

  @Override
  @SuppressWarnings("unchecked")
  public T decode(InputStream is) throws IOException {
    final DataInputStream dis = new DataInputStream(is);
    final int size = dis.readInt();
    final byte[] buffer = new byte[size];
    if (size != dis.read(buffer, 0, size)) {
      throw new IllegalStateException("This should never happen.");
    }
    return (T) kryo.get().readClassAndObject(new Input(new ByteArrayInputStream(buffer)));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // nop
  }

  // serialization

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    if (reinitialize.getAndSet(false)) {
      if (kryo == null) {
        kryo = ThreadLocal.withInitial(factory::apply);
      }
    }
    ois.defaultReadObject();
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
  }

}
