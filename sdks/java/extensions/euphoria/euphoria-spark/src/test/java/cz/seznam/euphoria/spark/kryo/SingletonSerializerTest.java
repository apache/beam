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
package cz.seznam.euphoria.spark.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SingletonSerializerTest {

  public static class TestSingleton {

    private static final TestSingleton INSTANCE = new TestSingleton();

    public static TestSingleton getInstance() {
      return INSTANCE;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  @Test
  public void test() {
    final Kryo kryo = new Kryo();
    kryo.register(TestSingleton.class, SingletonSerializer.of("getInstance"));

    final Output output = new Output(1024);
    kryo.writeClassAndObject(output, TestSingleton.getInstance());

    final Input input = new Input(output.getBuffer());
    final TestSingleton deserialized = (TestSingleton) kryo.readClassAndObject(input);

    assertEquals(TestSingleton.getInstance(), deserialized);
  }

}
